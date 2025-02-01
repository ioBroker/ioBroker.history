"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.HSmartDate = void 0;
exports.getSmartIntervals = getSmartIntervals;
class HSmartDate {
    // From 1 to 31
    date;
    // From 1 to 12
    month;
    // from 1970 to 2300
    year;
    hour;
    isLeapYear;
    dow; // 0 - sunday, 1 - monday
    // Number of weeks in month
    timeZone; // In hours
    lastSundayInMonth;
    end;
    summerTime;
    constructor(date, end) {
        this.year = date.year;
        this.month = date.month;
        this.date = date.date;
        // Time zone in hours. Germany +1
        if (date.timeZone === undefined) {
            this.timeZone = 1;
        }
        else {
            this.timeZone = date.timeZone;
        }
        this.end = !!end;
        const d = new Date(this.year, this.month - 1, this.date);
        const dow = d.getDay();
        this.hour = 0;
        this.dow = dow;
        this.isLeapYear = HSmartDate.isLeap(this.year);
        if (this.month === 3 || this.month === 10) {
            this.lastSundayInMonth = HSmartDate.getLastSundayOfMonth(this.year, this.month);
        }
        // calculate is day-saving time
        this.summerTime = HSmartDate.isDaylightSavingTime(d);
    }
    static isDaylightSavingTime(date) {
        const january = new Date(date.getFullYear(), 0, 1).getTimezoneOffset();
        const july = new Date(date.getFullYear(), 6, 1).getTimezoneOffset();
        return date.getTimezoneOffset() < Math.max(january, july);
    }
    static isLeap(year) {
        return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    }
    static getLastSundayOfMonth(year, month) {
        // Get the last day of the month
        const lastDayOfMonth = new Date(year, month, 0);
        // Calculate the day of the week (0 = Sunday, 1 = Monday, ..., 6 = Saturday)
        const dayOfWeek = lastDayOfMonth.getDay();
        // Calculate the date of the last Sunday
        const lastSunday = new Date(lastDayOfMonth);
        lastSunday.setDate(lastDayOfMonth.getDate() - dayOfWeek);
        return lastSunday.getDate();
    }
    getTime() {
        const offset = this.end ? -1 : 0;
        const tz = this.summerTime ? (this.timeZone + 1) * 3_600_000 : this.timeZone * 3_600_000;
        return new Date(Date.UTC(this.year, this.month - 1, this.date, this.hour, 0, 0)).getTime() - tz + offset;
    }
    getNextHour() {
        let result = 0;
        // Die mitteleuropäische Sommerzeit beginnt am letzten Sonntag im März um 2:00 Uhr MEZ, indem die Stundenzählung um eine Stunde von 2:00 Uhr auf 3:00 Uhr vorgestellt wird
        if (this.month === 3 && this.date === this.lastSundayInMonth && this.hour === 2) {
            this.summerTime = true; // summer time
            result = -1;
        }
        else if (this.month === 10 && this.date === this.lastSundayInMonth && this.hour === 3) {
            // Sie endet jeweils am letzten Sonntag im Oktober um 3:00 Uhr MESZ, indem die Stundenzählung um eine Stunde von 3:00 Uhr auf 2:00 Uhr zurückgestellt wird.
            this.summerTime = false; // winter time
            result = 1;
        }
        if (this.hour < 23) {
            this.hour++;
            return result;
        }
        this.hour = 0;
        this.dow++;
        if (this.dow > 6) {
            this.dow = 0;
        }
        if (this.date <= 27) {
            this.date++;
            return result;
        }
        if (this.month === 2) {
            if (this.isLeapYear) {
                if (this.date === 28) {
                    this.date = 29;
                    return result;
                }
            }
            this.date = 1;
            this.month = 3;
            this.lastSundayInMonth = HSmartDate.getLastSundayOfMonth(this.year, this.month);
            return result;
        }
        if (this.date < 30) {
            this.date++;
            return result;
        }
        // date is 30 or 31
        if (this.month === 1 ||
            this.month === 3 ||
            this.month === 5 ||
            this.month === 7 ||
            this.month === 8 ||
            this.month === 10 ||
            this.month === 12) {
            if (this.date < 31) {
                this.date++;
                return result;
            }
        }
        this.date = 1;
        this.month++;
        if (this.month === 10) {
            this.lastSundayInMonth = HSmartDate.getLastSundayOfMonth(this.year, this.month);
        }
        if (this.month > 12) {
            this.year++;
            this.isLeapYear = HSmartDate.isLeap(this.year);
            this.month = 1;
        }
        return result;
    }
    getMonth() {
        return this.month;
    }
}
exports.HSmartDate = HSmartDate;
function getSmartIntervals(startDate, intervalType, endDate, debug) {
    if (typeof startDate.timeZone !== 'number') {
        throw new Error('Time zone not provided!');
    }
    const start = new HSmartDate(startDate);
    let endTs;
    if (endDate) {
        endDate.timeZone = startDate.timeZone;
        const endDateObj = new HSmartDate(endDate, true);
        endTs = endDateObj.getTime();
    }
    else {
        endTs = Date.now();
    }
    let nStart;
    let nEnd;
    nStart = start.getTime();
    const result = [];
    if (endTs <= nStart) {
        return result;
    }
    if (intervalType === 'hour') {
        // It is simple. Get the first hour and add 60 minutes till the end
        do {
            nStart = start.getTime();
            start.getNextHour();
            nEnd = start.getTime();
            const interval = { start: nStart, end: nEnd };
            if (debug) {
                interval.startS = new Date(nStart).toISOString();
                interval.endS = new Date(nEnd).toISOString();
            }
            result.push(interval);
        } while (nEnd < endTs);
    }
    else if (intervalType === 'day') {
        do {
            nStart = start.getTime();
            const length = 24;
            for (let i = 0; i < length; i++) {
                start.getNextHour();
            }
            nEnd = start.getTime();
            const interval = { start: nStart, end: nEnd };
            if (debug) {
                interval.startS = new Date(nStart).toISOString();
                interval.endS = new Date(nEnd).toISOString();
            }
            result.push(interval);
        } while (nEnd < endTs);
    }
    else if (intervalType === 'month') {
        let m = start.getMonth();
        nEnd = nStart;
        do {
            nStart = nEnd;
            do {
                for (let i = 0; i < 24; i++) {
                    start.getNextHour();
                }
                if (m !== start.getMonth()) {
                    m = start.getMonth();
                    nEnd = start.getTime();
                    const interval = { start: nStart, end: nEnd };
                    if (debug) {
                        interval.startS = new Date(nStart).toISOString();
                        interval.endS = new Date(nEnd).toISOString();
                    }
                    result.push(interval);
                    break;
                }
                // eslint-disable-next-line no-constant-condition
            } while (true);
        } while (nEnd < endTs);
    }
    return result;
}
//# sourceMappingURL=time.js.map