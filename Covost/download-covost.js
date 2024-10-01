const puppeteer = require('puppeteer');
const hrefArrayMain=[];
const fs =require("fs")
let done=false;
(async () => {
  const browser = await puppeteer.launch({
     headless: false, slowMo: 100, // Uncomment to visualize test
  });
  const page = await browser.newPage();

  // Load "https://commonvoice.mozilla.org/en/datasets"
  await page.goto('https://commonvoice.mozilla.org/en/datasets');

  // Resize window to 1792 x 889
  await page.setViewport({ width: 1792, height: 889 });

  // Click on <select> "Abkhaz Afrikaans Albanian..."
  await page.waitForSelector('.input-row select');
  await page.click('.input-row select');

  // Fill "ta" on <select> .wrapper > [name="bundleLocale"]
  await page.waitForSelector('.wrapper > [name="bundleLocale"]');
//   await page.select('.wrapper > [name="bundleLocale"]', 'eng');

  // Scroll wheel by X:0, Y:8
  await page.evaluate(() => window.scrollBy(0, 8));

  // Scroll wheel by X:46, Y:348
  await page.evaluate(() => window.scrollBy(46, 348));

  // Scroll wheel by X:0, Y:142
  await page.evaluate(() => window.scrollBy(0, 142));

  // Scroll wheel by X:40, Y:144
  await page.evaluate(() => window.scrollBy(40, 144));

  // Scroll wheel by X:0, Y:2
  await page.evaluate(() => window.scrollBy(0, 2));

  // Scroll wheel by X:2, Y:0
  await page.evaluate(() => window.scrollBy(2, 0));

  // Scroll wheel by X:2, Y:2
  await page.evaluate(() => window.scrollBy(2, 2));

  // Scroll wheel by X:0, Y:2
  await page.evaluate(() => window.scrollBy(0, 2));

  for(let i=1;i<=27;i++){


    await page.waitForSelector('tr:nth-child(1) > .highlight:nth-child(1)');
    await page.click(`tr:nth-child(${i}) > .highlight:nth-child(1)`);


  // Scroll wheel by X:0, Y:4
  await page.evaluate(() => window.scrollBy(0, 4));

  // Scroll wheel by X:40, Y:292
  await page.evaluate(() => window.scrollBy(40, 292));

  // Scroll wheel by X:0, Y:1454
  await page.evaluate(() => window.scrollBy(0, 1454));

  // Scroll wheel by X:44, Y:416
  await page.evaluate(() => window.scrollBy(44, 416));

  // Scroll wheel by X:0, Y:1970
  await page.evaluate(() => window.scrollBy(0, 1970));

  // Scroll wheel by X:10, Y:14
  await page.evaluate(() => window.scrollBy(10, 14));

  // Scroll wheel by X:0, Y:-4
  await page.evaluate(() => window.scrollBy(0, -4));

  // Scroll wheel by X:-46, Y:-280
  await page.evaluate(() => window.scrollBy(-46, -280));

  // Scroll wheel by X:0, Y:-394
  await page.evaluate(() => window.scrollBy(0, -394));

  // Scroll wheel by X:-24, Y:-68
  await page.evaluate(() => window.scrollBy(-24, -68));

  // Scroll wheel by X:0, Y:-2
  await page.evaluate(() => window.scrollBy(0, -2));

   
await page.waitForSelector('.dataset-download-prompt:nth-child(3) [name="confirmSize"]');
await page.evaluate(() => {
    const checkbox = document.querySelector('.dataset-download-prompt:nth-child(3) [name="confirmSize"]');
    console.log(">>checkbox1>>>",checkbox);
    if (!checkbox.checked) {
        checkbox.click();
    }
});

// await page.click('.dataset-download-prompt:nth-child(3) [name="confirmSize"]');

// Click on <span> .dataset-download-prompt:nth-child(3) .labeled-checkbox:nth-child(3) > .checkbox-container
await page.waitForSelector('.dataset-download-prompt:nth-child(3) .labeled-checkbox:nth-child(3) > .checkbox-container');
await page.evaluate(() => {
    const checkbox = document.querySelector('.dataset-download-prompt:nth-child(3) .labeled-checkbox:nth-child(3) > .checkbox-container input');
    console.log(">>checkbox2>>>",checkbox);
    if (!checkbox.checked) {
        checkbox.click();
    }
});
// await page.click('.dataset-download-prompt:nth-child(3) .labeled-checkbox:nth-child(3) > .checkbox-container');

// Click on <input> .dataset-download-prompt:nth-child(3) [name="confirmJoinMailingList"]

// await page.click('.dataset-download-prompt:nth-child(3) [name="confirmJoinMailingList"]');


// Click on <input> .dataset-download-prompt:nth-child(3) [name="email"] > [name="email"]
await page.waitForSelector('.dataset-download-prompt:nth-child(3) [name="email"] > [name="email"]');
await page.click('.dataset-download-prompt:nth-child(3) [name="email"] > [name="email"]');

// Fill "test@mail.com" on <input> .dataset-download-prompt:nth-child(3) [name="email"] > [name="email"]
await page.waitForSelector('.dataset-download-prompt:nth-child(3) [name="email"] > [name="email"]:not([disabled])');
await page.evaluate(() => {
    const emailInput = document.querySelector('.dataset-download-prompt:nth-child(3) [name="email"] > [name="email"]');
    if (emailInput.value !== '') {
        emailInput.value = '';
    }
});

await page.type('.dataset-download-prompt:nth-child(3) [name="email"] > [name="email"]', "test@mail.com")

await page.waitForSelector('.dataset-download-prompt:nth-child(3) [name="confirmJoinMailingList"]');
await page.evaluate(() => {
    const checkbox = document.querySelector('.dataset-download-prompt:nth-child(3) [name="confirmJoinMailingList"]');
    console.log(">>checkbox3>>>",checkbox);
    if (!checkbox.checked) {
        checkbox.click();
    }
    else{
        checkbox.click();
        checkbox.click();
    }
});

  await page.waitForFunction(
    () => {
      const elements = document.querySelectorAll('a.button.rounded.download-language');
      return Array.from(elements).some(element => element.textContent.trim() === 'Download Dataset Bundle');
    },
    { timeout: 30000 } // Adjust the timeout as needed
  );

  // Extract the href from the button and add it to an array
  const hrefArray = await page.evaluate(() => {
    const elements = document.querySelectorAll('a.button.rounded.download-language');
    const targetElements = Array.from(elements).filter(element => element.textContent.trim() === 'Download Dataset Bundle');
    return targetElements.map(element => element.href);
  });

  hrefArrayMain.push(hrefArray[0])
 

  }
 
  console.log(hrefArrayMain);

  // Function to write JSON data to a file
function writeJsonToFile(data, path) {
    fs.writeFile(path, JSON.stringify(data, null, 2), (err) => {
        if (err) {
            console.error('Error writing JSON to file:', err);
        } else {
            console.log('JSON data written to file successfully.');
        }
    });
}

writeJsonToFile(hrefArrayMain,"output.json")
  
  await browser.close();
})();