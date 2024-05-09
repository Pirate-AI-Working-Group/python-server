const { BloomFilter, CombinedBloomFilter } = require("./bloomfilter.js");
const fs = require("fs");
const { promisify } = require("util");
const path = require("path");
const readFile = promisify(fs.readFile);

const bloomFilters = [];

async function loadBloomFilter(name) {
  const filePath = path.join(__dirname, "data", `${name}.dat`);
  console.log(filePath);
  try {
    let arrayBuffer = (await readFile(filePath)).buffer;
    const combined = new CombinedBloomFilter();
    combined.name = name;
    combined.parts = [
      new BloomFilter(new Int32Array(arrayBuffer.slice(0, 287552)), 20),
      new BloomFilter(new Int32Array(arrayBuffer.slice(287552)), 21),
    ];
    bloomFilters.push(combined);
    // Rest of your code goes here
  } catch (error) {
    // Handle file read error
    console.error(`Error reading file: ${error}`);
  }
}

let bloomFiltersLoadedPromise = (async () => {
  await loadBloomFilter("transphobic");
  await loadBloomFilter("t-friendly");
})();

async function check_transphobic(value) {
  let is_transphobic = false;
  let is_trans_friendly = false;
  console.log(value);
  await bloomFiltersLoadedPromise;
  for (const bloomFilter of bloomFilters) {
    if (bloomFilter.test(value) & (bloomFilter.name == "t-friendly")) {
      is_trans_friendly = true;
    } else if (bloomFilter.test(value) & (bloomFilter.name == "transphobic")) {
      is_transphobic = true;
    }
  }
  console.log("is_trans_friendly", is_trans_friendly);
  console.log("is_transphobic", is_transphobic);
  if (is_transphobic & is_trans_friendly) {
    return 0;
  } else if (is_transphobic) {
    return 2;
  } else if (is_trans_friendly) {
    return 1;
  } else {
    return 0;
  }
}
module.exports.check_transphobic = check_transphobic;
