'use strict';

const async = require('async');
const winston = require('winston');
const db = require('../../database');
const batch = require('../../batch');
const topics = require('../../topics');

module.exports = {
	name: 'Users post count per tid',
	timestamp: Date.UTC(2016, 3, 19),

	method: function (callback) {
		const count = 0;
		batch.processSortedSet('topics:tid', processTids.bind(null, count), {}, callback);
	},
};

function processTids(count, tids, next) {
	winston.verbose(`upgraded ${count} topics`);
	count += tids.length;
	async.each(tids, processTid, next);
}

function processTid(tid, next) {
	db.delete(`tid:${tid}:posters`, (err) => {
		if (err) {
			return next(err);
		}
		topics.getPids(tid, processPids.bind(null, tid, next));
	});
}

function processPids(tid, next, err, pids) {
	if (err) {
		return next(err);
	}
	if (!pids.length) {
		return next();
	}
	async.eachSeries(pids, processPid.bind(null, tid), next);
}

function processPid(tid, pid, next) {
	db.getObjectField(`post:${pid}`, 'uid', (err, uid) => {
		if (err) {
			return next(err);
		}
		if (!parseInt(uid, 10)) {
			return next();
		}
		db.sortedSetIncrBy(`tid:${tid}:posters`, 1, uid, next);
	});
}
