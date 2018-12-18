const moment = require('moment')
require('moment-timezone')
const nSchedule = require('node-schedule-tz')
const EventEmitter = require('events').EventEmitter

const HOUR_IN_MS = 60 * 60 * 1000
const DAY_IN_MS = 24 * HOUR_IN_MS

const log = () => {}

function startInterval(secs, cb) {
  cb()
  return setInterval(cb, secs * 1000)
}

function calcCronExpr(mode, number) {
  if (mode === 'HOURLY') {
    return `0 ${number} * * * *`
  } else {
    return `0 0 ${number} * * *`
  }
}

class CronDaemon {
  constructor(getDocs, triggerDoc, { mode, number, logger } = {}) {
    this.log = logger || log
    this.jobDocs = {}
    this.jobMap = {}

    this.getDocs = getDocs
    this.triggerDoc = triggerDoc
    this.opts = { mode, number: number || 0 }

    this.syncDocs = this.syncDocs.bind(this)
    this.scanForFailedPublishes = this.scanForFailedPublishes.bind(this)
    this.setupJob = this.setupJob.bind(this)
    this.removeJob = this.removeJob.bind(this)
    this.trigger = this.trigger.bind(this)
  }

  async start() {
    startInterval(60, this.syncDocs)
    startInterval(10, this.scanForFailedPublishes)
  }

  async syncDocs() {
    this.log('Syncing docs.')

    const current = Object.keys(this.jobDocs).map(k => this.jobDocs[k])
    const latest = await this.getDocs()

    const cIds = new Set(current.map(d => d.id))
    const nIds = new Set(latest.map(d => d.id))

    const disabledIds = Array.from(new Set([...cIds].filter(x => !nIds.has(x))))

    const newDocs = Array.from(
      new Set([...nIds].filter(x => !cIds.has(x))),
    ).map(id => latest.filter(d => d.id === id)[0])

    disabledIds.forEach(this.removeJob)
    newDocs.forEach(this.setupJob)
  }

  async scanForFailedPublishes() {
    this.log('Scanning for failed publishes.')

    const issues = []
    const docList = Object.keys(this.jobDocs).map(k => this.jobDocs[k])

    docList.forEach(doc => {
      const timezone = doc.timezone || 'UTC'
      const nowLocal = moment(moment.tz(timezone).format('YYYY-MM-DDTHH:mm:ss'))
      const localCP = moment(doc.checkpoint).tz(timezone)
      const checkpointM = moment(localCP.format('YYYY-MM-DDTHH:mm:ss'))
      const number = doc.number || this.opts.number || 0
      let mCount

      if (this.opts.mode === 'HOURLY') {
        let upperCP
        if (checkpointM.minute() < number) {
          upperCP = checkpointM
            .add(1, 'hour')
            .startOf('hour')
            .subtract(60 - number, 'minutes')
            .add(5, 'minutes')
        } else {
          upperCP = checkpointM
            .add(1, 'hour')
            .startOf('hour')
            .add(number, 'minutes')
            .add(5, 'minutes')
        }

        const diffInHours = Math.floor(nowLocal.diff(upperCP) / HOUR_IN_MS)
        mCount = Math.max(diffInHours + 1, 0)
      } else {
        let upperCP
        if (checkpointM.hour() < number) {
          upperCP = checkpointM
            .add(1, 'day')
            .startOf('day')
            .subtract(24 - number, 'hours')
            .add(5, 'minutes')
        } else {
          upperCP = checkpointM
            .add(1, 'day')
            .startOf('day')
            .add(number, 'hours')
            .add(5, 'minutes')
        }

        const diffInDays = Math.floor(nowLocal.diff(upperCP) / DAY_IN_MS)
        mCount = Math.max(diffInDays + 1, 0)
      }

      if (doc.runSince < mCount) issues.push(doc)
    })

    await Promise.all(issues.map(d => this.trigger(d, true)))
  }

  setupJob(doc) {
    const f = () => this.trigger(doc, false)
    const number = doc.number || this.opts.number || 0
    const cronExpr = calcCronExpr(this.opts.mode, number)
    const timezone = doc.timezone || 'UTC'

    this.log(`Setting up job: ${doc.id} - ${cronExpr} - ${timezone}.`)

    this.jobDocs[doc.id] = doc
    this.jobMap[doc.id] = nSchedule.scheduleJob(doc.id, cronExpr, timezone, f)
  }

  removeJob(id) {
    this.log(`Removing job: ${id}.`)

    this.jobMap[id].cancel()
    delete this.jobMap[id]
    delete this.jobDocs[id]
  }

  async trigger(doc, isRetry) {
    const now = moment.utc().format('YYYY-MM-DDTHH:mm:ss')
    const data = isRetry
      ? { checkpoint: now, runSince: 0 }
      : { runSince: doc.runSince + 1 }

    this.log(`Triggering job: ${doc.id} - isRetry[${isRetry}].`)

    await this.triggerDoc(doc.id, data)
    this.jobDocs[doc.id] = Object.assign(doc, data)
  }
}

module.exports = CronDaemon
