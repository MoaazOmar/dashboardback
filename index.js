const express = require('express');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

// Cache for dimension tables
let cachedDimDateEcom, cachedDimDateWeather, cachedDimCountry, cachedDimCustomer, cachedDimWeather, cachedDimOrder;

// Streaming CSV reader with filter and processing callback
function readCSV(filePath, processRow, filter = () => true) {
  return new Promise((resolve, reject) => {
    let rowCount = 0;
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        if (filter(row)) {
          try {
            processRow(row, rowCount++);
          } catch (err) {
            console.error(`Error processing row ${rowCount} in ${filePath}:`, err);
          }
        }
      })
      .on('end', () => {
        console.log(`Finished reading ${filePath}, processed ${rowCount} rows`);
        resolve();
      })
      .on('error', (error) => {
        console.error(`Error reading ${filePath}:`, error);
        reject(error);
      });
  });
}

// Load dimension tables at startup
async function loadDimensionTables() {
  try {
    const loadCSVToArray = (filePath) => {
      return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
          .pipe(csv())
          .on('data', (data) => results.push(data))
          .on('end', () => resolve(results))
          .on('error', (error) => reject(error));
      });
    };

    console.log('Loading dimension tables...');
    cachedDimDateEcom = await loadCSVToArray(path.join(__dirname, 'csv/dim_date_ecom.csv'));
    cachedDimDateWeather = await loadCSVToArray(path.join(__dirname, 'csv/dim_date_weather.csv'));
    cachedDimCountry = await loadCSVToArray(path.join(__dirname, 'csv/dim_country.csv'));
    cachedDimCustomer = await loadCSVToArray(path.join(__dirname, 'csv/Dim_Customer.csv'));
    cachedDimWeather = await loadCSVToArray(path.join(__dirname, 'csv/dim_weather.csv'));
    cachedDimOrder = await loadCSVToArray(path.join(__dirname, 'csv/Dim_Order_Cat.csv'));
    console.log('Dimension tables loaded into memory');
  } catch (error) {
    console.error('Failed to load dimension tables:', error);
    throw error;
  }
}

// Middleware
app.use(cors({
  origin: "*",
  methods: ["GET", "POST", "PUT", "DELETE"],
  allowedHeaders: ["Content-Type", "Authorization"]
}));
app.get('/favicon.ico', (req, res) => res.status(204));

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy' });
});

// Root endpoint
app.get("/", (req, res) => {
  res.send("✅ Backend is running on Railway!");
});

// /weather-sales endpoint
app.get('/weather-sales', async (req, res) => {
  console.log('Starting /weather-sales');
  try {
    // Build maps from cached data
    const ecomDateMap = {};
    cachedDimDateEcom.forEach(d => {
      ecomDateMap[d.surr_key_date_of_ecom] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    const weatherDateMap = {};
    cachedDimDateWeather.forEach(d => {
      weatherDateMap[d.surr_key_date_weather] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    const weatherInfoMap = {};
    cachedDimWeather.forEach(w => {
      weatherInfoMap[w.surr_key_Weather_info] = w.Weather_Category;
    });

    const countryWeatherMap = {};
    cachedDimCountry.forEach(c => {
      countryWeatherMap[c.surr_key_country_weather] = c.Country;
    });

    const customerEcomMap = {};
    cachedDimCustomer.forEach(c => {
      customerEcomMap[c.surr_key_customer] = c.Country;
    });

    // Stream fact_weather.csv
    const weatherMap = {};
    await readCSV(
      path.join(__dirname, 'csv/fact_weather.csv'),
      (w) => {
        const date = weatherDateMap[w.surr_key_date_weather];
        const country = countryWeatherMap[w.surr_key_country_weather];
        if (date && country === 'United Kingdom') {
          weatherMap[date] = weatherInfoMap[w.surr_key_Weather_info] || 'Unknown';
        }
      },
      (w) => countryWeatherMap[w.surr_key_country_weather] === 'United Kingdom'
    );

    // Stream fact_sales.csv
    const grouped = {};
    await readCSV(
      path.join(__dirname, 'csv/fact_sales.csv'),
      (sale) => {
        const date = ecomDateMap[sale.surr_key_date_of_ecom];
        const country = customerEcomMap[sale.surr_key_customer];
        if (date && country === 'United Kingdom') {
          const weatherCategory = weatherMap[date] || 'Unknown';
          const price = Number(sale.TotalPrice) || 0;
          grouped[weatherCategory] = (grouped[weatherCategory] || 0) + price;
        }
      },
      (sale) => customerEcomMap[sale.surr_key_customer] === 'United Kingdom'
    );

    const result = Object.entries(grouped).map(([category, totalSales]) => ({
      category,
      totalSales
    }));

    res.json(result);
  } catch (error) {
    console.error('Error in /weather-sales:', error);
    res.status(500).json({ error: 'مشكلة في معالجة البيانات' });
  }
});

// /Temp_range-orders endpoint
app.get('/Temp_range-orders', async (req, res) => {
  console.log('Starting /Temp_range-orders');
  try {
    const ecomDateMap = {};
    cachedDimDateEcom.forEach(d => {
      ecomDateMap[d.surr_key_date_of_ecom] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    const weatherDateMap = {};
    cachedDimDateWeather.forEach(w => {
      weatherDateMap[w.surr_key_date_weather] = `${w.Year}-${w.Month}-${w.Day}`;
    });

    const TempRange = {};
    cachedDimWeather.forEach(t => {
      TempRange[t.surr_key_Weather_info] = t.temp_range;
    });

    const orders = {};
    cachedDimOrder.forEach(o => {
      orders[o.surr_key_order_cat] = o.Order_Value_Category;
    });

    const countryWeatherMap = {};
    cachedDimCountry.forEach(c => {
      if (c.Country === 'United Kingdom') {
        countryWeatherMap[c.surr_key_country_weather] = c.Country;
      }
    });

    const customerEcomMap = {};
    cachedDimCustomer.forEach(c => {
      if (c.Country === 'United Kingdom') {
        customerEcomMap[c.surr_key_customer] = c.Country;
      }
    });

    const weatherMap = {};
    await readCSV(
      path.join(__dirname, 'csv/fact_weather.csv'),
      (w) => {
        const date = weatherDateMap[w.surr_key_date_weather];
        const country = countryWeatherMap[w.surr_key_country_weather];
        if (date && country === 'United Kingdom') {
          weatherMap[date] = TempRange[w.surr_key_Weather_info] || 'Unknown';
        }
      },
      (w) => countryWeatherMap[w.surr_key_country_weather] === 'United Kingdom'
    );

    const grouped = {};
    await readCSV(
      path.join(__dirname, 'csv/fact_sales.csv'),
      (sale) => {
        const date = ecomDateMap[sale.surr_key_date_of_ecom];
        const country = customerEcomMap[sale.surr_key_customer];
        if (date && country === 'United Kingdom') {
          const tempRange = weatherMap[date] || 'Unknown';
          const orderCategory = orders[sale.surr_key_order_cat] || 'Unknown';
          const key = `${tempRange}_${orderCategory}`;
          if (!grouped[key]) {
            grouped[key] = {
              tempRange,
              orderValueCategory: orderCategory,
              orderCount: 0
            };
          }
          grouped[key].orderCount += 1;
        }
      },
      (sale) => customerEcomMap[sale.surr_key_customer] === 'United Kingdom'
    );

    const result = Object.values(grouped).map(item => ({
      x: parseFloat(item.tempRange) || 0,
      y: item.orderCount,
      type: item.orderValueCategory
    }));

    res.json(result);
  } catch (error) {
    console.error('Error in /Temp_range-orders:', error);
    res.status(500).json({ error: 'Failed to process Temp_range-orders data' });
  }
});

// /rain-Intensity-vs-Returns endpoint
app.get('/rain-Intensity-vs-Returns', async (req, res) => {
  console.log('Starting /rain-Intensity-vs-Returns');
  try {
    const ecomDateMap = {};
    cachedDimDateEcom.forEach(d => {
      ecomDateMap[d.surr_key_date_of_ecom] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    const weatherDateMap = {};
    cachedDimDateWeather.forEach(w => {
      weatherDateMap[w.surr_key_date_weather] = `${w.Year}-${w.Month}-${w.Day}`;
    });

    const rainIntensity = {};
    cachedDimWeather.forEach(t => {
      rainIntensity[t.surr_key_Weather_info] = t.Rain_Intensity;
    });

    const orders = {};
    cachedDimOrder.forEach(o => {
      orders[o.surr_key_order_cat] = o.Is_Return?.toLowerCase() === 'true' ? 'Returned' : 'Not Returned';
    });

    const countryWeatherMap = {};
    cachedDimCountry.forEach(c => {
      if (c.Country === 'United Kingdom') {
        countryWeatherMap[c.surr_key_country_weather] = c.Country;
      }
    });

    const customerEcomMap = {};
    cachedDimCustomer.forEach(c => {
      if (c.Country === 'United Kingdom') {
        customerEcomMap[c.surr_key_customer] = c.Country;
      }
    });

    const weatherMap = {};
    await readCSV(
      path.join(__dirname, 'csv/fact_weather.csv'),
      (w) => {
        const date = weatherDateMap[w.surr_key_date_weather];
        const country = countryWeatherMap[w.surr_key_country_weather];
        if (date && country === 'United Kingdom') {
          weatherMap[date] = rainIntensity[w.surr_key_Weather_info] || 'Unknown';
        }
      },
      (w) => countryWeatherMap[w.surr_key_country_weather] === 'United Kingdom'
    );

    const grouped = {};
    await readCSV(
      path.join(__dirname, 'csv/fact_sales.csv'),
      (sale) => {
        const date = ecomDateMap[sale.surr_key_date_of_ecom];
        const country = customerEcomMap[sale.surr_key_customer];
        if (date && country === 'United Kingdom') {
          const rain = weatherMap[date] || 'Unknown';
          const returnStatus = orders[sale.surr_key_order_cat] || 'Not Returned';
          const key = `${rain}_${returnStatus}`;
          if (!grouped[key]) {
            grouped[key] = {
              rainIntensity: rain,
              returnStatus,
              transactions: 0,
              quantities: 0,
              revenue: 0
            };
          }
          grouped[key].transactions += 1;
          grouped[key].quantities += Number(sale.Quantity) || 0;
          grouped[key].revenue += Number(sale.TotalPrice) || 0;
        }
      },
      (sale) => customerEcomMap[sale.surr_key_customer] === 'United Kingdom'
    );

    const result = Object.values(grouped);
    res.json(result);
  } catch (error) {
    console.error('Error in /rain-Intensity-vs-Returns:', error);
    res.status(500).json({ error: 'Failed to process rain-Intensity-vs-Returns data' });
  }
});

// /season_Sales-vs-totalAmountSales endpoint
app.get('/season_Sales-vs-totalAmountSales', async (req, res) => {
  console.log('Starting /season_Sales-vs-totalAmountSales');
  try {
    const ecomDateMap = {};
    cachedDimDateEcom.forEach(d => {
      ecomDateMap[d.surr_key_date_of_ecom] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    const weatherDateMap = {};
    cachedDimDateWeather.forEach(w => {
      weatherDateMap[w.surr_key_date_weather] = `${w.Year}-${w.Month}-${w.Day}`;
    });

    const weatherSeasonMap = {};
    cachedDimDateWeather.forEach(s => {
      weatherSeasonMap[s.surr_key_date_weather] = s.Season;
    });

    const countryWeatherMap = {};
    cachedDimCountry.forEach(c => {
      if (c.Country === 'United Kingdom') {
        countryWeatherMap[c.surr_key_country_weather] = c.Country;
      }
    });

    const customerEcomMap = {};
    cachedDimCustomer.forEach(c => {
      if (c.Country === 'United Kingdom') {
        customerEcomMap[c.surr_key_customer] = c.Country;
      }
    });

    const seasonMap = {};
    await readCSV(
      path.join(__dirname, 'csv/fact_weather.csv'),
      (w) => {
        const date = weatherDateMap[w.surr_key_date_weather];
        const country = countryWeatherMap[w.surr_key_country_weather];
        if (date && country === 'United Kingdom') {
          seasonMap[date] = weatherSeasonMap[w.surr_key_date_weather] || 'Unknown';
        }
      },
      (w) => countryWeatherMap[w.surr_key_country_weather] === 'United Kingdom'
    );

    const grouped = {};
    await readCSV(
      path.join(__dirname, 'csv/fact_sales.csv'),
      (sale) => {
        const date = ecomDateMap[sale.surr_key_date_of_ecom];
        const country = customerEcomMap[sale.surr_key_customer];
        if (date && country === 'United Kingdom') {
          const season = seasonMap[date] || 'Unknown';
          if (!grouped[season]) {
            grouped[season] = { season, totalSales: 0 };
          }
          grouped[season].totalSales += parseFloat(sale.TotalPrice) || 0;
        }
      },
      (sale) => customerEcomMap[sale.surr_key_customer] === 'United Kingdom'
    );

    const result = Object.values(grouped);
    res.json(result);
  } catch (error) {
    console.error('Error in /season_Sales-vs-totalAmountSales:', error);
    res.status(500).json({ error: 'Failed to process season_Sales-vs-totalAmountSales data' });
  }
});

// /sunshineHours_Purchase endpoint
app.get('/sunshineHours_Purchase', async (req, res) => {
  console.log('Starting /sunshineHours_Purchase');
  try {
    const ecomDateMap = {};
    cachedDimDateEcom.forEach(d => {
      ecomDateMap[d.surr_key_date_of_ecom] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    const weatherDateMap = {};
    cachedDimDateWeather.forEach(w => {
      weatherDateMap[w.surr_key_date_weather] = `${w.Year}-${w.Month}-${w.Day}`;
    });

    const countryWeatherMap = {};
    cachedDimCountry.forEach(c => {
      if (c.Country === 'United Kingdom') {
        countryWeatherMap[c.surr_key_country_weather] = c.Country;
      }
    });

    const customerEcomMap = {};
    cachedDimCustomer.forEach(c => {
      if (c.Country === 'United Kingdom') {
        customerEcomMap[c.surr_key_customer] = c.Country;
      }
    });

    const sunshineHoursMap = {};
    await readCSV(
      path.join(__dirname, 'csv/fact_weather.csv'),
      (w) => {
        const date = weatherDateMap[w.surr_key_date_weather];
        const country = countryWeatherMap[w.surr_key_country_weather];
        if (date && country === 'United Kingdom') {
          sunshineHoursMap[date] = parseFloat(w.Sunshine_Hours || 0);
        }
      },
      (w) => countryWeatherMap[w.surr_key_country_weather] === 'United Kingdom'
    );

    const grouped = {};
    await readCSV(
      path.join(__dirname, 'csv/fact_sales.csv'),
      (sale) => {
        const date = ecomDateMap[sale.surr_key_date_of_ecom];
        const country = customerEcomMap[sale.surr_key_customer];
        if (date && country === 'United Kingdom') {
          const sunshine = sunshineHoursMap[date];
          if (sunshine !== undefined) {
            if (!grouped[sunshine]) {
              grouped[sunshine] = { sunshineHours: sunshine, purchaseCount: 0 };
            }
            grouped[sunshine].purchaseCount += 1;
          }
        }
      },
      (sale) => customerEcomMap[sale.surr_key_customer] === 'United Kingdom'
    );

    const result = Object.values(grouped).map(item => ({
      sunshineHours: parseFloat(item.sunshineHours.toFixed(2)),
      purchaseCount: item.purchaseCount
    }));

    res.json(result);
  } catch (error) {
    console.error('Error in /sunshineHours_Purchase:', error);
    res.status(500).json({ error: 'Failed to process sunshineHours_Purchase data' });
  }
});

// Start server after loading dimension tables
loadDimensionTables().then(() => {
  app.listen(PORT, () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
  });
}).catch((error) => {
  console.error('Failed to start server:', error);
  process.exit(1);
});