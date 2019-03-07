const express = require('express');
const app = express();
const bodyParser = require('body-parser');
const cors = require('cors');
const request = require('request');
const secret = require('./secret');

const NO_SECONDS_IN_HOUR = 3600;
const NO_SECONDS_IN_WEEK = 7 * 24 * NO_SECONDS_IN_HOUR;

const {Datastore} = require('@google-cloud/datastore');
const datastore = new Datastore({
    projectId: 'bookworm-221210'
});

const memjs = require('memjs');
const cacheClient = memjs.Client.create(`${secret.cacheUser}:${secret.cachePassword}@${secret.cacheEndpoint}`, {
	expires: NO_SECONDS_IN_WEEK
});

const corsOptions = {
	origin: /bookworm-221210.appspot.com/i
};

app.use(cors(corsOptions));

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
	extended: false
}));

app.get('/', (req, res) => {
	res.send('<h1>Integrator</h1>').end();
});

app.get('/goodreads/refresh', async (req, res) => {
	//TODO: check if refresh is not needed
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
			let requestUrl = `${baseUrl}${isbns}&key=${secret.goodreadsKey}`;
			
			setTimeout(() => {
				request({
					url: requestUrl,
					json: true
				}, function (error, response, body) {
					if (response.statusCode == 200) {
						for (bookStatistic of body.books)  {
							let isbn;
							if (bookStatistic.isbn13)
								isbn = bookStatistic.isbn13;
							else
								isbn = bookStatistic.isbn;

							if (isbn) {
								let goodreadsData = JSON.parse(JSON.stringify(bookStatistic));
								const toOmit = ['ratings_count', 'reviews_count', 'text_reviews_count', 'reviews_count', 'work_text_reviews_count'];
								omitProperties(goodreadsData, toOmit);
								cacheClient.set(isbn, JSON.stringify(goodreadsData), { expires: NO_SECONDS_IN_WEEK }, (err, success) => {
								});
							}
						}
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

function omitProperties(obj, props) {
	for (let prop of props) {
		if (obj.hasOwnProperty(prop)) {
			delete obj[prop];
		}
	}
}


/**
 * @returns 200 if in cache
 * 			404 if not in cache
 * 			400 if cannot make anything of request
 */
app.get('/:provider/:isbn', async (req, res) => {
	if (req.params.provider == 'goodreads') {
		const isbn = req.params.isbn;

		let {value, flags} = await cacheClient.get(isbn);

		if (value === null && flags === null) {
			res.status(404).end();
		} else {
			let reviewStats = JSON.parse(value.toString('utf-8'));
			res.status(200).send(reviewStats).end();
		}
	} else {
		res.status(400).end();
	}
});

const PORT = process.env.PORT || 8081;
app.listen(PORT, () => {
	console.log(`Integrator server started on port ${PORT}`);
});
