const express = require('express');
const app = express();
const bodyParser = require('body-parser');
const request = require('request');

const NO_SECONDS_IN_HOUR = 3600;
const NO_SECONDS_IN_WEEK = 7 * 24 * NO_SECONDS_IN_HOUR;
const NodeCache = require('node-cache');

const goodreadsKey = require('./secret').goodreadsKey;
const goodreadsCache = new NodeCache({
	stdTTL: NO_SECONDS_IN_WEEK,
	checkperiod: NO_SECONDS_IN_HOUR
});

const {Datastore} = require('@google-cloud/datastore');
const datastore = new Datastore({
    projectId: 'bookworm-221210'
});

let lastRefreshKeys = 0;
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
	extended: false
}));

app.get('/', (req, res) => {
	res.send('<h1>Integrator</h1>').end();
});

app.get('/goodreads/refresh', async (req, res) => {
	if (lastRefreshKeys && lastRefreshKeys == goodreadsCache.stats.keys) {
		res.status(304).end();
		return;
	}

	res.status(201).end();

	const baseUrl = 'https://www.goodreads.com/book/review_counts.json?isbns=';
	const MAX_ITEMS_IN_BATCH = 500;
	const GOODREADS_DELAY_MS = 2000;

	let getBatchOfIsbnsQuery = datastore.createQuery('Book')
		.select(['isbn'])
		.filter('isbn', '>', '')
		.limit(MAX_ITEMS_IN_BATCH);

	let [entities, info] = await datastore.runQuery(getBatchOfIsbnsQuery);	

	let iter = 0;
	while (true) {
		console.log(`Request number ${++iter}`);
		if (info.moreResults !== Datastore.NO_MORE_RESULTS) {
			let isbns = entities.map(item => item.isbn).join('%2C');
			let requestUrl = `${baseUrl}${isbns}&key=${goodreadsKey}`;
			
			setTimeout(() => {
				request({
					url: requestUrl,
					json: true
				}, function (error, response, body) {
					if (response.statusCode == 200) {
						body.books.forEach(bookStatistic => {
							let isbn;
							if (bookStatistic.isbn13)
								isbn = bookStatistic.isbn13;
							else
								isbn = bookStatistic.isbn;

							if (isbn) {
								goodreadsCache.set(isbn, bookStatistic);
							}
						});
						lastRefreshKeys = goodreadsCache.stats.keys;
					} else {
						console.error(`Received response status code: ${response.statusCode}`);
					}
				});
			}, GOODREADS_DELAY_MS);

			getBatchOfIsbnsQuery = getBatchOfIsbnsQuery.start(info.endCursor);
			[entities, info] = await datastore.runQuery(getBatchOfIsbnsQuery);
		} else {
			break;
		}
	}
	console.log('Finished sending requests to goodreads for statistics');
});

/**
 * @returns 200 if in cache
 * 			204 if not in cache
 * 			400 if cannot make anything of request
 */
app.get('/:provider/:isbn', (req, res) => {
	if (req.params.provider == 'goodreads') {
		const isbn = req.params.isbn;
		let reviewStats = goodreadsCache.get(isbn);
		
		if (reviewStats == undefined) {
			res.status(204).end();
		} else {
			res.status(200).send(reviewStats).end();
		}
	} else {
		res.status(400).end();
	}
});

const PORT = process.env.PORT || 8081;
app.listen(PORT, () => {
	console.log(`App listening on port ${PORT}`);
});