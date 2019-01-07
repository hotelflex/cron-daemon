const moment = require('moment')
require('moment-timezone')

const HOUR_IN_MS = 60 * 60 * 1000
const DAY_IN_MS = 24 * HOUR_IN_MS

function startInterval(secs, cb) {
  cb()
  return setInterval(cb, secs * 1000)
}

class CronDaemon {
  constructor(getDocs, triggerDoc, { mode, number } = {}) {
    this.docMap = {}

    this.getDocs = getDocs
    this.triggerDoc = triggerDoc
    this.opts = { mode, number: number || 0 }

    this.syncDocs = this.syncDocs.bind(this)
    this.scanDocs = this.scanDocs.bind(this)
    this.trigger = this.trigger.bind(this)
  }

  async start() {
    startInterval(60, this.syncDocs)
    startInterval(15, this.scanDocs)
  }

  async syncDocs() {
    const docs = await this.getDocs()
    const docMap = {}

    docs.forEach(doc => {
      docMap[doc.id] = doc
    })

    this.docMap = docMap
  }

  async scanDocs() {
    const docs = Object.keys(this.docMap).map(k => this.docMap[k])
    const toTrigger = []

    docs.forEach(doc => {
      const timezone = doc.timezone || 'UTC'
      const nowLocal = moment.utc(
        moment.tz(timezone).format('YYYY-MM-DDTHH:mm:ss'),
      )
      const localCP = moment.utc(doc.checkpoint).tz(timezone)
      const checkpointM = moment.utc(localCP.format('YYYY-MM-DDTHH:mm:ss'))
      const number = doc.number || this.opts.number || 0
      let mCount

      if (this.opts.mode === 'HOURLY') {
        let upperCP
        if (checkpointM.minute() < number) {
          upperCP = checkpointM
            .add(1, 'hour')
            .startOf('hour')
            .subtract(60 - number, 'minutes')
        } else {
          upperCP = checkpointM
            .add(1, 'hour')
            .startOf('hour')
            .add(number, 'minutes')
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
        } else {
          upperCP = checkpointM
            .add(1, 'day')
            .startOf('day')
            .add(number, 'hours')
        }

        const diffInDays = Math.floor(nowLocal.diff(upperCP) / DAY_IN_MS)
        mCount = Math.max(diffInDays + 1, 0)
      }

      if (doc.runSince < mCount) toTrigger.push(doc)
    })

    await Promise.all(toTrigger.map(d => this.trigger(d, true)))
  }

  async trigger(doc, isRetry) {
    const now = moment.utc().format('YYYY-MM-DDTHH:mm:ss')
    const data = isRetry
      ? { checkpoint: now, runSince: 0 }
      : { runSince: doc.runSince + 1 }

    await this.triggerDoc(doc.id, data)
    this.docMap[doc.id] = Object.assign(doc, data)
  }
}

module.exports = CronDaemon
