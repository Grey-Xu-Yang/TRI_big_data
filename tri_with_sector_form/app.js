'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

// Scan function for the 'greyxu_tri_sum' HBase table.
hclient.table('greyxu_tri_sum').scan(
	{
		filter: {
			type: "PrefixFilter",
			value: "2121" // Example industry_sector_code
		},
		maxVersions: 1
	},
	function (err, cells) {
		if (err) {
			console.error(err);
			return;
		}
		console.info(cells);
		console.info(groupByYear("2121", cells)); // Use the industry sector code as a prefix here.
	}
);

// Modified removePrefix function to account for the new key structure.
function removePrefix(text, prefix) {
	if(text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}

// Modified rowToMap function to convert a row to a map/object.
function rowToMap(row) {
	var result = {};
	row.forEach(function (cell) {
		// Assuming the HBase table uses 'sector' as the column family.
		let qualifier = cell['column'].split(':')[1];
		// Convert the binary data to a float.
		let value = Buffer.from(cell['$'], 'binary').readInt32BE(0);
		result[qualifier] = value;
	});
	return result;
}

// Modified groupByYear function to accumulate data by year.
function groupByYear(sectorCode, cells) {
	let result = [];
	let totals = {};
	function roundToDecimalPlaces(number, decimalPlaces) {
		const multiplier = Math.pow(10, decimalPlaces);
		return Math.round(number * multiplier) / multiplier;
	}
	cells.forEach(function (cell) {
		let keyParts = cell['key'].split('_');
		let year = keyParts[1]; // The year is now the first part of the key.

		if (!totals[year]) {
			totals[year] = {};
			}
			let columnQualifier = removePrefix(cell['column'], 'sector:');
			totals[year][columnQualifier] = roundToDecimalPlaces(Number(cell['$']), 2);
		});

	// Convert the totals object into an array for output.
	for (let year in totals) {
		let yearData = totals[year];
		yearData.year = year;
		result.push(yearData);
	}
	// Sort the results by year in descending order.
	result.sort((a, b) => b.year - a.year);
	//console.log(result)
	return result;
}

// Now GroupBY Timestamp as shown in the speed layer.
function groupByTimestamp(sectorCode, cells) {
	let result = [];
	let totals = {};

	cells.forEach(function (cell) {
		let keyParts = cell['key'].split('_');
		let timestamp = keyParts[1]; //  timestamp is the second part of the key

		if (!totals[timestamp]) {
			totals[timestamp] = {};
		}
		let rowData = rowToMap([cell]);
		let columnQualifier = Object.keys(rowData)[0];
		totals[timestamp][columnQualifier] = rowData[columnQualifier];
	});

	for (let timestamp in totals) {
		let timestampData = totals[timestamp];
		timestampData.timestamp = timestamp;
		result.push(timestampData);
	}

	result.sort((a, b) => b.timestamp - a.timestamp);
	return result;
}

app.use(express.static('public'));
app.get('/sector-data.html', function (req, res) {
	const sectorCode = req.query['sector'];
	//console.log(sectorCode);

	// Ensure the table name matches your HBase table for the sector data
	hclient.table('greyxu_tri_sum').scan(
		{
			filter: {
				type: "PrefixFilter",
				value: sectorCode
			},
			maxVersions: 1
		},
		function (err, cells) {
			if (err) {
				console.error(err);
				res.status(500).send("Error querying HBase");
				return;
			}
			// Make sure to adjust the path to your actual Mustache template file
			let template = filesystem.readFileSync("result.mustache").toString();
			console.log(cells)
			let data = groupByYear(sectorCode, cells);
			let html = mustache.render(template, { sectorData: data });
			res.send(html);
		}
	);
});

/* Send simulated weather to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);

app.get('/new_toxic.html',function (req, res) {
	var sector_val = req.query['sector'];
	var releaseOnSite = parseInt(req.query['release_on']) || 0;
	var releaseOffSite = parseInt(req.query['release_off']) || 0;
	var energyOnSite = parseInt(req.query['energy_on']) || 0;
	var energyOffSite = parseInt(req.query['energy_off']) || 0;
	var recyclingOnSite = parseInt(req.query['recycle_on']) || 0;
	var recyclingOffSite = parseInt(req.query['recycle_off']) || 0;
	var treatmentOnSite = parseInt(req.query['treat_on']) || 0;
	var treatmentOffSite = parseInt(req.query['treat_off']) || 0;
	var productionWaste = parseInt(req.query['product_waste']) || 0;

	var report = {
		sectorCode: sector_val,
		on_site_release_total: releaseOnSite,
		off_site_release_total: releaseOffSite,
		energy_recover_on: energyOnSite,
		energy_recover_of: energyOffSite,
		recycling_on_site: recyclingOnSite,
		recycling_off_site: recyclingOffSite,
		treatment_on_site: treatmentOnSite,
		treatment_off_site: treatmentOffSite,
		production_waste: productionWaste
	};

	kafkaProducer.send([{ topic: 'greyxu_tri', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log(report);
			res.redirect('submit-tri.html');
		});
});

// Endpoint to get data from 'greyxu_latest_tri' HBase table.
app.get('/latest-sector-data.html', function (req, res) {
	const sectorCode = req.query['sector'];

	hclient.table('greyxu_latest_tri').scan({
		filter: {
			type: "PrefixFilter",
			value: sectorCode
		}
	}, function (err, cells) {
		if (err) {
			console.error(err);
			res.status(500).send("Error querying HBase");
			return;
		}
		// Process and group cells by timestamp
		let groupedData = groupByTimestamp(sectorCode, cells);

		let template = filesystem.readFileSync("latest_result.mustache").toString();
		let html = mustache.render(template, { latestSectorData: groupedData });
		res.send(html);
	});
});

app.listen(port);
