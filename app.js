const csv = require('csv-parser');
const fs = require('fs');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const csvCountryWriter = createCsvWriter({
  path: 'output/filteredCountry.csv',
  header: [
    {id: 'SKU', title: 'SKU'},
    {id: 'DESCRIPTION', title: 'DESCRIPTION'},
    {id: 'YEAR', title: 'YEAR'},
    {id: 'CAPACITY', title: 'CAPACITY'},
    {id: 'URL', title: 'URL'},
    {id: 'PRICE', title: 'PRICE'},
    {id: 'SELLER_INFORMATION', title: 'SELLER_INFORMATION'},
    {id: 'OFFER_DESCRIPTION', title: 'OFFER_DESCRIPTION'},
    {id: 'COUNTRY', title: 'COUNTRY'},
  ]
});

const csvMinPriceWriter = createCsvWriter({
  path: 'output/lowestPrice.csv',
  header: [
    {id: 'SKU', title: 'SKU'},
    {id: 'FIRST_MINIMUM_PRICE', title: 'FIRST_MINIMUM_PRICE'},
    {id: 'SECOND_MINIMUM_PRICE', title: 'SECOND_MINIMUM_PRICE'},
  ]
});

let array = [];
let minimumArray = [];

function minMax(items) {
  const filteredItems = items.filter(j => j && j.PRICE.indexOf('$') !== -1);
    return filteredItems.reduce((acc, val) => {
      acc.SKU = val.SKU;
      if (acc.FIRST_MINIMUM_PRICE === undefined) {
        acc.SECOND_MINIMUM_PRICE = val.PRICE;
        acc.FIRST_MINIMUM_PRICE = val.PRICE;
      } else if (Number(val.PRICE.replace(/[^0-9]+/g, "")) < Number(acc.FIRST_MINIMUM_PRICE.replace(/[^0-9]+/g, ""))) {  
        acc.SECOND_MINIMUM_PRICE = acc.FIRST_MINIMUM_PRICE;
        acc.FIRST_MINIMUM_PRICE = val.PRICE;
      } else if (
        Number(val.PRICE.replace(/[^0-9]+/g, "")) > Number(acc.SECOND_MINIMUM_PRICE.replace(/[^0-9]+/g, "")) &&
        Number(acc.SECOND_MINIMUM_PRICE.replace(/[^0-9]+/g, "")) === Number(acc.FIRST_MINIMUM_PRICE.replace(/[^0-9]+/g, ""))
      ) {
        acc.SECOND_MINIMUM_PRICE = val.PRICE;
      } else if (
        Number(val.PRICE.replace(/[^0-9]+/g, "")) < Number(acc.SECOND_MINIMUM_PRICE.replace(/[^0-9]+/g, "")) &&
        Number(val.PRICE.replace(/[^0-9]+/g, "")) > Number(acc.FIRST_MINIMUM_PRICE.replace(/[^0-9]+/g, ""))
      ) {
        acc.SECOND_MINIMUM_PRICE = val.PRICE;
      }
      return acc;
    }, {});
}

function readUSAFile() {
  fs.createReadStream('output/filteredCountry.csv')
    .pipe(csv())
    .on('data', (row) => {
      minimumArray.push(row);
    })
    .on('end', () => {
      let minPrice = [];
      let groupBySKU = minimumArray.reduce((acc, value) => {
        if (!acc[value.SKU]) {
          acc[value.SKU] = [];
        }
       
        acc[value.SKU].push(value);
       
        return acc;
      }, {});
      Object.values(groupBySKU).map(i => {
        minPrice.push(minMax(i));
      })
     
      csvMinPriceWriter
        .writeRecords(minPrice)
        .then(()=> console.log('The CSV file was written successfully'));
    });
}

fs.createReadStream('input/main.csv')
  .pipe(csv())
  .on('data', (row) => {
    array.push(row);
  })
  .on('end', () => {
    const filtereddArray = array.filter(item => item.COUNTRY.indexOf('USA') !==-1)
   
    csvCountryWriter
      .writeRecords(filtereddArray)
      .then(()=> {
        readUSAFile();
        console.log('The CSV file was written successfully')
      });
  });
