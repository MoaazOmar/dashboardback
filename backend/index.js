const express = require('express');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const cors = require('cors');
const app = express();
const PORT = process.env.PORT || 3000;

// حل مشكلة favicon.ico 404
app.get('/favicon.ico', (req, res) => res.status(204));
app.use(cors());
app.use(cors({
  origin: "*",
  methods: ["GET", "POST", "PUT", "DELETE"],
  allowedHeaders: ["Content-Type", "Authorization"]
}));
// دالة قراءة CSV
function readCSV(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => resolve(results))
      .on('error', (error) => reject(error));
  });
}

app.get('/weather-sales', async (req, res) => {
  try {
    // 1. قراءة ملفات CSV
    const sales = await readCSV(path.join(__dirname, 'csv/fact_sales.csv'));
    const dimDateEcom = await readCSV(path.join(__dirname, 'csv/dim_date_ecom.csv'));
    const weather = await readCSV(path.join(__dirname, 'csv/fact_weather.csv'));
    const dimDateWeather = await readCSV(path.join(__dirname, 'csv/dim_date_weather.csv'));
    const dimWeather = await readCSV(path.join(__dirname, 'csv/dim_weather.csv'));
    const dimCountry = await readCSV(path.join(__dirname, 'csv/dim_country.csv'));
    const DimCustomer = await readCSV(path.join(__dirname, 'csv/Dim_Customer.csv'));

    // 2. خريطة تواريخ المبيعات
    const ecomDateMap = {};
    dimDateEcom.forEach(d => {
      ecomDateMap[d.surr_key_date_of_ecom] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    // 3. خريطة تواريخ الطقس
    const weatherDateMap = {};
    dimDateWeather.forEach(d => {
      weatherDateMap[d.surr_key_date_weather] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    // 4. خريطة فئات الطقس
    const weatherInfoMap = {};
    dimWeather.forEach(w => {
      weatherInfoMap[w.surr_key_Weather_info] = w.Weather_Category;
    });

    // 5. خريطة الدول (للطقس)
    const countryWeatherMap = {};
    dimCountry.forEach(c => {
      countryWeatherMap[c.surr_key_country_weather] = c.Country;
    });

    // 6. خريطة العملاء (للمبيعات)
    const customerEcomMap = {};
    DimCustomer.forEach(c => {
      customerEcomMap[c.surr_key_customer] = c.Country;
    });

    // 7. ربط الطقس بالتاريخ (فلتر لـ United Kingdom)
    const weatherMap = {};
    weather.forEach(w => {
      const date = weatherDateMap[w.surr_key_date_weather];
      const country = countryWeatherMap[w.surr_key_country_weather];
      if (date && country === 'United Kingdom') {
        weatherMap[date] = weatherInfoMap[w.surr_key_Weather_info] || 'Unknown';
      }
    });

    // 8. جمع المبيعات حسب الطقس (فلتر لـ United Kingdom)
    const grouped = sales.reduce((acc, sale) => {
      const date = ecomDateMap[sale.surr_key_date_of_ecom];
      const country = customerEcomMap[sale.surr_key_customer];
      if (date && country === 'United Kingdom') {
        const weatherCategory = weatherMap[date] || 'Unknown';
        const price = Number(sale.TotalPrice) || 0;
        acc[weatherCategory] = (acc[weatherCategory] || 0) + price;
      }
      return acc;
    }, {});

    // 9. تحويل النتيجة لمصفوفة
    const result = Object.entries(grouped).map(([category, totalSales]) => ({
      category,
      totalSales
    }));

    // 10. إرجاع النتيجة
    res.json(result);

  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: 'مشكلة في معالجة البيانات'
    });
  }
});

app.get('/Temp_range-orders', async (req, res) => {
  try {
    const sales = await readCSV(path.join(__dirname, 'csv/fact_sales.csv'));
    const dimDateEcom = await readCSV(path.join(__dirname, 'csv/dim_date_ecom.csv'));
    const weather = await readCSV(path.join(__dirname, 'csv/fact_weather.csv'));
    const dimDateWeather = await readCSV(path.join(__dirname, 'csv/dim_date_weather.csv'));
    const dimCountry = await readCSV(path.join(__dirname, 'csv/dim_country.csv'));
    const dimCustomer = await readCSV(path.join(__dirname, 'csv/Dim_Customer.csv'));
    const dimWeather = await readCSV(path.join(__dirname, 'csv/dim_weather.csv'));
    const dimOrder = await readCSV(path.join(__dirname, 'csv/Dim_Order_Cat.csv'));

    // 1. خريطة تواريخ المبيعات
    const ecomDateMap = {};
    dimDateEcom.forEach(d => {
      ecomDateMap[d.surr_key_date_of_ecom] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    // 2. خريطة تواريخ الطقس
    const weatherDateMap = {};
    dimDateWeather.forEach(w => {
      weatherDateMap[w.surr_key_date_weather] = `${w.Year}-${w.Month}-${w.Day}`;
    });

    // 3. خريطة temp_range
    const TempRange = {};
    dimWeather.forEach(t => {
      const fixedTempRange = t.temp_range
      TempRange[t.surr_key_Weather_info] = fixedTempRange;
    });

    // 4. خريطة order categories
    const orders = {};
    dimOrder.forEach(o => {
      orders[o.surr_key_order_cat] = o.Order_Value_Category;
    });

    // 5. خريطة الدول (للـ weather)
    const countryWeatherMap = {};
    dimCountry.forEach(c => {
      if (c.Country === 'United Kingdom') {
        countryWeatherMap[c.surr_key_country_weather] = c.Country;
      }
    });

    // 6. خريطة العملاء (للـ sales)
    const customerEcomMap = {};
    dimCustomer.forEach(c => {
      if (c.Country === 'United Kingdom') {
        customerEcomMap[c.surr_key_customer] = c.Country;
      }
    });

    // 7. ربط الطقس بالتاريخ (مع temp_range)
    const weatherMap = {};
    weather.forEach(w => {
      const date = weatherDateMap[w.surr_key_date_weather];
      const country = countryWeatherMap[w.surr_key_country_weather];
      if (date && country === 'United Kingdom') {
        weatherMap[date] = TempRange[w.surr_key_Weather_info] || 'Unknown';
      }
    });

    // 8. Reduce: نحسب عدد الطلبات بدل من total sales
    const grouped = sales.reduce((acc, sale) => {
      const date = ecomDateMap[sale.surr_key_date_of_ecom];
      const country = customerEcomMap[sale.surr_key_customer];
      if (date && country === 'United Kingdom') {
        const tempRange = weatherMap[date] || 'Unknown';
        const orderCategory = orders[sale.surr_key_order_cat] || 'Unknown';

        const key = `${tempRange}_${orderCategory}`;
        if (!acc[key]) {
          acc[key] = {
            tempRange,
            orderValueCategory: orderCategory,
            orderCount: 0
          };
        }
        acc[key].orderCount += 1;
      }
      return acc;
    }, {});

    // 9. Convert to array
    const result = Object.values(grouped).map(item => ({
      x: parseFloat(item.tempRange), // درجة الحرارة على المحور X
      y: item.orderCount, // عدد الطلبات على المحور Y
      type: item.orderValueCategory // نوع الطلب (Small/Medium/Large)
    }));


    res.json(result);

  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: 'Failed to process Temp_range-orders data'
    });
  }
});
app.get('/rain-Intensity-vs-Returns', async (req, res) => {
  try {
    const sales = await readCSV(path.join(__dirname, 'csv/fact_sales.csv'));
    const weather = await readCSV(path.join(__dirname, 'csv/fact_weather.csv'));
    const dimDateEcom = await readCSV(path.join(__dirname, 'csv/dim_date_ecom.csv'));
    const dimDateWeather = await readCSV(path.join(__dirname, 'csv/dim_date_weather.csv'));
    const dimCountry = await readCSV(path.join(__dirname, 'csv/dim_country.csv'));
    const dimCustomer = await readCSV(path.join(__dirname, 'csv/Dim_Customer.csv'));
    const dimOrder = await readCSV(path.join(__dirname, 'csv/Dim_Order_Cat.csv'));
    const dimWeather = await readCSV(path.join(__dirname, 'csv/dim_weather.csv'));

    // 1. خريطة تواريخ المبيعات
    const ecomDateMap = {};
    dimDateEcom.forEach(d => {
      ecomDateMap[d.surr_key_date_of_ecom] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    // 2. خريطة تواريخ الطقس
    const weatherDateMap = {};
    dimDateWeather.forEach(w => {
      weatherDateMap[w.surr_key_date_weather] = `${w.Year}-${w.Month}-${w.Day}`;
    });

    // 3. خريطة Rain_Intensity
    const rainIntensity = {};
    dimWeather.forEach(t => {
      rainIntensity[t.surr_key_Weather_info] = t.Rain_Intensity;
    });

    // 4. خريطة لاستخراج الاوردرات اذا كانت استرجعت ام لا
const orders = {};
dimOrder.forEach(o => {
  orders[o.surr_key_order_cat] = 
    (o.Is_Return && o.Is_Return.toLowerCase() === 'true') 
    ? 'Returned' 
    : 'Not Returned';
});

    // 5. خريطة الدول (للـ weather)
    const countryWeatherMap = {};
    dimCountry.forEach(c => {
      if (c.Country === 'United Kingdom') {
        countryWeatherMap[c.surr_key_country_weather] = c.Country;
      }
    });

    // 6. خريطة العملاء (للـ sales)
    const customerEcomMap = {};
    dimCustomer.forEach(c => {
      if (c.Country === 'United Kingdom') {
        customerEcomMap[c.surr_key_customer] = c.Country;
      }
    });

    // 7. ربط الطقس بالتاريخ
    const weatherMap = {};
    weather.forEach(w => {
      const date = weatherDateMap[w.surr_key_date_weather];
      const country = countryWeatherMap[w.surr_key_country_weather];
      if (date && country === 'United Kingdom') {
        weatherMap[date] = rainIntensity[w.surr_key_Weather_info] || 'Unknown';
      }
    });

// 8. التجميع حسب Rain_Intensity + Return Status
const grouped = sales.reduce((acc, sale) => {
  const date = ecomDateMap[sale.surr_key_date_of_ecom];
  const country = customerEcomMap[sale.surr_key_customer];

  if (date && country === 'United Kingdom') {
    const rain = weatherMap[date] || 'Unknown';
    const returnStatus = orders[sale.surr_key_order_cat] || 'Not Returned';

    const key = `${rain}_${returnStatus}`;
    if (!acc[key]) {
      acc[key] = { 
        rainIntensity: rain, 
        returnStatus, 
        transactions: 0,   // عدد العمليات
        quantities: 0,     // الكميات
        revenue: 0         // الإيرادات
      };
    }

    acc[key].transactions += 1;                 // Transaction count
    acc[key].quantities += Number(sale.Quantity); // Quantity
    acc[key].revenue += Number(sale.TotalPrice);  // Revenue
  }
  return acc;
}, {});


    // 9. تحويل النتيجة Array
    const result = Object.values(grouped);
    res.json(result);

  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Failed to process rain-Intensity-vs-Returns data' });
  }
});
app.get('/season_Sales-vs-totalAmountSales', async (req, res) => {
  try {
    const sales = await readCSV(path.join(__dirname, 'csv/fact_sales.csv'));
    const weather = await readCSV(path.join(__dirname, 'csv/fact_weather.csv'));
    const dimDateEcom = await readCSV(path.join(__dirname, 'csv/dim_date_ecom.csv'));
    const dimDateWeather = await readCSV(path.join(__dirname, 'csv/dim_date_weather.csv'));
    const dimCountry = await readCSV(path.join(__dirname, 'csv/dim_country.csv'));
    const dimCustomer = await readCSV(path.join(__dirname, 'csv/Dim_Customer.csv'));

    // 1. خريطة تواريخ المبيعات
    const ecomDateMap = {};
    dimDateEcom.forEach(d => {
      ecomDateMap[d.surr_key_date_of_ecom] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    // 2. خريطة تواريخ الطقس
    const weatherDateMap = {};
    dimDateWeather.forEach(w => {
      weatherDateMap[w.surr_key_date_weather] = `${w.Year}-${w.Month}-${w.Day}`;
    });

    // 3. خريطة المواسم
    const weatherSeasonMap = {};
    dimDateWeather.forEach(s => {
      weatherSeasonMap[s.surr_key_date_weather] = s.Season;
    });

    // 4. خريطة الدول (للـ weather)
    const countryWeatherMap = {};
    dimCountry.forEach(c => {
      if (c.Country === 'United Kingdom') {
        countryWeatherMap[c.surr_key_country_weather] = c.Country;
      }
    });

    // 5. خريطة العملاء (للـ sales)
    const customerEcomMap = {};
    dimCustomer.forEach(c => {
      if (c.Country === 'United Kingdom') {
        customerEcomMap[c.surr_key_customer] = c.Country;
      }
    });

    // 6. ربط الطقس بالتاريخ → موسم
    const seasonMap = {};
    weather.forEach(w => {
      const date = weatherDateMap[w.surr_key_date_weather];
      const country = countryWeatherMap[w.surr_key_country_weather];
      if (date && country === 'United Kingdom') {
        seasonMap[date] = weatherSeasonMap[w.surr_key_date_weather] || 'Unknown';
      }
    });

    // 7. تجميع المبيعات حسب Season
    const grouped = sales.reduce((acc, sale) => {
      const date = ecomDateMap[sale.surr_key_date_of_ecom];
      const country = customerEcomMap[sale.surr_key_customer];

      if (date && country === 'United Kingdom') {
        const season = seasonMap[date] || 'Unknown';
        if (!acc[season]) {
          acc[season] = { season, totalSales: 0 };
        }
        acc[season].totalSales += parseFloat(sale.TotalPrice) || 0;
      }
      return acc;
    }, {});

    // 8. تحويل النتيجة Array
    const result = Object.values(grouped);
    res.json(result);

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to process season_Sales-vs-totalAmountSales data' });
  }
});
app.get('/sunshineHours_Purchase', async (req, res) => {
  try {
    const sales = await readCSV(path.join(__dirname, 'csv/fact_sales.csv'));
    const weather = await readCSV(path.join(__dirname, 'csv/fact_weather.csv'));
    const dimDateEcom = await readCSV(path.join(__dirname, 'csv/dim_date_ecom.csv'));
    const dimDateWeather = await readCSV(path.join(__dirname, 'csv/dim_date_weather.csv'));
    const dimCountry = await readCSV(path.join(__dirname, 'csv/dim_country.csv'));
    const dimCustomer = await readCSV(path.join(__dirname, 'csv/Dim_Customer.csv'));

    // 1. خريطة تواريخ المبيعات
    const ecomDateMap = {};
    dimDateEcom.forEach(d => {
      ecomDateMap[d.surr_key_date_of_ecom] = `${d.Year}-${d.Month}-${d.Day}`;
    });

    // 2. خريطة تواريخ الطقس
    const weatherDateMap = {};
    dimDateWeather.forEach(w => {
      weatherDateMap[w.surr_key_date_weather] = `${w.Year}-${w.Month}-${w.Day}`;
    });

    // 3. خريطة الدول (للـ weather)
    const countryWeatherMap = {};
    dimCountry.forEach(c => {
      if (c.Country === 'United Kingdom') {
        countryWeatherMap[c.surr_key_country_weather] = c.Country;
      }
    });

    // 4. خريطة العملاء (للـ sales)
    const customerEcomMap = {};
    dimCustomer.forEach(c => {
      if (c.Country === 'United Kingdom') {
        customerEcomMap[c.surr_key_customer] = c.Country;
      }
    });

    // 5. خريطة Sunshine_Hours بالتاريخ
    const sunshineHoursMap = {};
    weather.forEach(w => {
      const date = weatherDateMap[w.surr_key_date_weather];
      const country = countryWeatherMap[w.surr_key_country_weather];
      if (date && country === 'United Kingdom') {
        sunshineHoursMap[date] = parseFloat(w.Sunshine_Hours || 0);
      }
    });

    // 6. تجميع عدد المعاملات حسب Sunshine_Hours
    const grouped = {};
    sales.forEach(sale => {
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
    });

    const result = Object.values(grouped).map(item => ({
  sunshineHours: parseFloat(item.sunshineHours.toFixed(2)),
  purchaseCount: item.purchaseCount
    }));

    res.json(result);

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to process sunshineHours_Purchase data' });
  }
});

app.get("/", (req, res) => {
  res.send("✅ Backend is running on Railway!");
});


app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});