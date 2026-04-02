/**
 * shared-db.js
 * Firestore-backed data layer for IBSH School Leave Management System
 * International Bilingual School at Hsinchu Science Park
 *
 * Exposed as window.AppDB
 */

(function () {
  'use strict';

  // ---------------------------------------------------------------------------
  // Constants
  // ---------------------------------------------------------------------------

  const LEGACY_STORAGE_KEYS = [
    'ibsh_students',
    'ibsh_leaves',
    'ibsh_tardies',
    'ibsh_holidays',
    'ibsh_settings',
  ];

  const COLLECTIONS = {
    students: 'students',
    leaves: 'leaves',
    tardies: 'tardies',
    holidays: 'holidays',
    settings: 'settings',
  };

  const SETTINGS_DOC_ID = 'app';
  const BATCH_LIMIT = 400;

  const PERIODS = [
    { id: 1,   label: 'Period 1', start: '08:10', end: '09:00' },
    { id: 2,   label: 'Period 2', start: '09:10', end: '10:00' },
    { id: 3,   label: 'Period 3', start: '10:10', end: '11:00' },
    { id: 4,   label: 'Period 4', start: '11:10', end: '12:00' },
    { id: 'L', label: 'Lunch',    start: '12:00', end: '13:00' },
    { id: 5,   label: 'Period 5', start: '13:10', end: '14:00' },
    { id: 6,   label: 'Period 6', start: '14:10', end: '15:00' },
    { id: 7,   label: 'Period 7', start: '15:20', end: '16:10' },
  ];

  const ACADEMIC_PERIODS = PERIODS.filter(period => period.id !== 'L');

  const DEFAULT_SETTINGS = {
    currentSchoolYear: '',
    gradeYearOffset: 0,
  };

  const DEFAULT_LEAVE_TIMES = {
    fromTime: '08:10',
    toTime: '16:10',
  };

  const CLASS_NAME_COLLATOR = new Intl.Collator('zh-Hant-TW', {
    numeric: true,
    sensitivity: 'base',
  });

  // ---------------------------------------------------------------------------
  // Internal state
  // ---------------------------------------------------------------------------

  let _app = null;
  let _db = null;
  let _initPromise = null;
  let _writeQueue = Promise.resolve();

  let _students = [];
  let _leaves = [];
  let _tardies = [];
  let _holidays = [];
  let _settings = { ...DEFAULT_SETTINGS };

  let _status = {
    mode: 'memory',
    error: null,
  };

  // ---------------------------------------------------------------------------
  // Helpers – persistence
  // ---------------------------------------------------------------------------

  function _clearLegacyStorage() {
    try {
      LEGACY_STORAGE_KEYS.forEach(key => localStorage.removeItem(key));
    } catch (error) {
      console.warn('[AppDB] Failed to clear legacy localStorage keys', error);
    }
  }

  function _resetState() {
    _students = [];
    _leaves = [];
    _tardies = [];
    _holidays = [];
    _settings = { ...DEFAULT_SETTINGS };
  }

  function _emitStatus() {
    window.dispatchEvent(new CustomEvent('app-db-status', {
      detail: status(),
    }));
  }

  function _setStatus(mode, error) {
    _status = {
      mode: mode || 'memory',
      error: error || null,
    };
    _emitStatus();
  }

  function _getFirebaseNamespace() {
    return window.firebase || null;
  }

  function _getFirebaseConfig() {
    return window.__FIREBASE_CONFIG__ || null;
  }

  function _stripUndefined(value) {
    if (Array.isArray(value)) {
      return value.map(item => _stripUndefined(item));
    }
    if (!value || typeof value !== 'object') {
      return value;
    }

    const result = {};
    Object.keys(value).forEach(key => {
      if (value[key] !== undefined) {
        result[key] = _stripUndefined(value[key]);
      }
    });
    return result;
  }

  function _queueWrite(task) {
    const run = _writeQueue.catch(() => undefined).then(async () => {
      if (!_db) return null;
      return task();
    });

    _writeQueue = run.catch(error => {
      console.error('[AppDB] Firebase write failed:', error);
      _setStatus(_db ? 'firebase' : 'memory', error && error.message ? error.message : 'Firebase write failed.');
    });

    return run;
  }

  async function _commitChunkedSet(collectionName, records) {
    if (!_db || !records || records.length === 0) return;

    for (let start = 0; start < records.length; start += BATCH_LIMIT) {
      const batch = _db.batch();
      records.slice(start, start + BATCH_LIMIT).forEach(record => {
        const clean = _stripUndefined(record);
        const docId = clean.id || _generateId();
        delete clean.id;
        batch.set(_db.collection(collectionName).doc(docId), clean, { merge: true });
      });
      await batch.commit();
    }
  }

  async function _commitChunkedDelete(collectionName, ids) {
    if (!_db || !ids || ids.length === 0) return;

    for (let start = 0; start < ids.length; start += BATCH_LIMIT) {
      const batch = _db.batch();
      ids.slice(start, start + BATCH_LIMIT).forEach(id => {
        batch.delete(_db.collection(collectionName).doc(id));
      });
      await batch.commit();
    }
  }

  async function _loadCollection(collectionName, normalizer) {
    if (!_db) return [];
    const snapshot = await _db.collection(collectionName).get();
    return snapshot.docs
      .map(doc => normalizer({ id: doc.id, ...doc.data() }))
      .filter(Boolean);
  }

  async function _loadAllData() {
    const [
      students,
      leaves,
      tardies,
      holidays,
      settingsSnap,
    ] = await Promise.all([
      _loadCollection(COLLECTIONS.students, _normalizeStudentRecord),
      _loadCollection(COLLECTIONS.leaves, _normalizeLeaveRecord),
      _loadCollection(COLLECTIONS.tardies, _normalizeTardyRecord),
      _loadCollection(COLLECTIONS.holidays, _normalizeHolidayRecord),
      _db.collection(COLLECTIONS.settings).doc(SETTINGS_DOC_ID).get(),
    ]);

    _students = students;
    _leaves = leaves;
    _tardies = tardies;
    _holidays = holidays;
    _settings = {
      ...DEFAULT_SETTINGS,
      ...(settingsSnap.exists ? settingsSnap.data() : {}),
    };
  }

  // ---------------------------------------------------------------------------
  // Helpers – general
  // ---------------------------------------------------------------------------

  function _generateId() {
    return Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  }

  function _defaultEmail(studentNo) {
    return studentNo ? String(studentNo).trim() + '@ibsh.tw' : '';
  }

  function _asDate(value) {
    if (!value) return null;
    if (value instanceof Date) {
      return Number.isNaN(value.getTime()) ? null : value;
    }
    if (value && typeof value.toDate === 'function') {
      const parsed = value.toDate();
      return Number.isNaN(parsed.getTime()) ? null : parsed;
    }
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  function _toDateStr(value) {
    if (!value) return '';
    if (typeof value === 'string') {
      return value.slice(0, 10);
    }
    const parsed = _asDate(value);
    return parsed ? parsed.toISOString().slice(0, 10) : '';
  }

  function _toTimeStr(value) {
    if (!value) return '';
    if (typeof value === 'string') {
      const match = value.match(/(\d{2}:\d{2})/);
      return match ? match[1] : value.slice(0, 5);
    }
    const parsed = _asDate(value);
    if (!parsed) return '';
    return String(parsed.getHours()).padStart(2, '0') + ':' + String(parsed.getMinutes()).padStart(2, '0');
  }

  function _toIsoString(value, fallback) {
    if (!value) return fallback || '';
    if (typeof value === 'string') return value;
    const parsed = _asDate(value);
    return parsed ? parsed.toISOString() : (fallback || '');
  }

  function _normalizeDateTime(value, fallbackDate, fallbackTime) {
    if (typeof value === 'string' && value) {
      if (value.includes('T')) return value.slice(0, 16);
      if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(value)) {
        return value.slice(0, 10) + 'T' + value.slice(11, 16);
      }
    }

    const parsed = _asDate(value);
    if (parsed) {
      return parsed.toISOString().slice(0, 16);
    }

    const dateStr = _toDateStr(fallbackDate);
    const timeStr = _toTimeStr(fallbackTime);
    return dateStr ? dateStr + 'T' + (timeStr || '00:00') : '';
  }

  function _timeToMinutes(timeStr) {
    const [hours, minutes] = String(timeStr || '00:00').split(':').map(Number);
    return (hours || 0) * 60 + (minutes || 0);
  }

  function _compareClassNames(a, b) {
    return CLASS_NAME_COLLATOR.compare(String(a || ''), String(b || ''));
  }

  function _parsePeriods(value) {
    if (typeof value === 'number' && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === 'string') {
      const match = value.match(/\d+/);
      return match ? parseInt(match[0], 10) : 0;
    }
    return 0;
  }

  function _normalizeStudentRecord(student) {
    if (!student) return null;

    const studentNo = String(student.studentNo || student.studentId || '').trim();
    const record = {
      id: student.id || _generateId(),
      chineseName: String(student.chineseName || student.studentName || '').trim(),
      studentNo: studentNo,
      studentId: studentNo,
      className: String(student.className || student.studentClass || '').trim(),
      seatNo: String(student.seatNo || '').trim(),
      englishName: String(student.englishName || '').trim(),
      email: String(student.email || _defaultEmail(studentNo)).trim(),
    };

    if (!record.studentNo && !record.chineseName && !record.className) {
      return null;
    }

    return record;
  }

  function _normalizeLeaveRecord(record) {
    if (!record) return null;

    const studentNo = String(record.studentNo || record.studentId || '').trim();
    const className = String(record.className || record.studentClass || '').trim();
    const leaveType = String(record.leaveType || record.leaveReason || '').trim();
    const reason = String(record.reason || record.parentNote || '').trim();
    const fromDate = _toDateStr(record.fromDate || record.startTime);
    const toDate = _toDateStr(record.toDate || record.endTime || fromDate);
    const fromTime = _toTimeStr(record.fromTime || record.startTime || DEFAULT_LEAVE_TIMES.fromTime) || DEFAULT_LEAVE_TIMES.fromTime;
    const toTime = _toTimeStr(record.toTime || record.endTime || DEFAULT_LEAVE_TIMES.toTime) || DEFAULT_LEAVE_TIMES.toTime;
    const startTime = _normalizeDateTime(record.startTime, fromDate, fromTime);
    const endTime = _normalizeDateTime(record.endTime, toDate || fromDate, toTime);
    const createdAt = _toIsoString(record.createdAt, _toIsoString(record.submittedAt, new Date().toISOString()));
    const periods = _parsePeriods(record.periods != null ? record.periods : record.totalPeriods);

    return {
      id: record.id || _generateId(),
      studentNo: studentNo,
      studentId: studentNo,
      className: className,
      studentClass: className,
      seatNo: String(record.seatNo || '').trim(),
      chineseName: String(record.chineseName || record.studentName || '').trim(),
      englishName: String(record.englishName || '').trim(),
      leaveType: leaveType,
      leaveReason: leaveType,
      fromDate: fromDate,
      toDate: toDate || fromDate,
      fromTime: fromTime,
      toTime: toTime,
      fullDay: Boolean(record.fullDay),
      startTime: startTime,
      endTime: endTime,
      periods: periods,
      totalPeriods: periods,
      reason: reason,
      parentNote: reason,
      deanSignature: String(record.deanSignature || '').trim(),
      doctorNote: Boolean(record.doctorNote),
      status: String(record.status || 'pending').trim(),
      createdAt: createdAt,
      submittedAt: _toIsoString(record.submittedAt, createdAt),
    };
  }

  function _normalizeTardyRecord(record) {
    if (!record) return null;

    const studentNo = String(record.studentNo || record.studentId || '').trim();
    return {
      id: record.id || _generateId(),
      studentNo: studentNo,
      studentId: studentNo,
      className: String(record.className || record.studentClass || '').trim(),
      seatNo: String(record.seatNo || '').trim(),
      chineseName: String(record.chineseName || record.studentName || '').trim(),
      englishName: String(record.englishName || '').trim(),
      reason: String(record.reason || '').trim(),
      date: _toDateStr(record.date || record.createdAt || new Date()),
      createdAt: _toIsoString(record.createdAt, new Date().toISOString()),
    };
  }

  function _normalizeHolidayRecord(holiday) {
    if (!holiday) return null;

    const startDate = _toDateStr(holiday.startDate || holiday.date);
    let endDate = _toDateStr(holiday.endDate || holiday.startDate || holiday.date);
    if (!startDate) return null;
    if (!endDate || endDate < startDate) endDate = startDate;

    return {
      id: holiday.id || _generateId(),
      date: startDate,
      startDate: startDate,
      endDate: endDate,
      name: String(holiday.name || '').trim(),
      type: String(holiday.type || 'manual').trim(),
    };
  }

  function _cloneSettings() {
    return {
      ...DEFAULT_SETTINGS,
      ..._settings,
    };
  }

  // ---------------------------------------------------------------------------
  // Initialisation
  // ---------------------------------------------------------------------------

  async function _initFirebase() {
    const firebaseNs = _getFirebaseNamespace();
    const config = _getFirebaseConfig();

    if (!firebaseNs || !config) {
      throw new Error('Firebase config or SDK is missing.');
    }
    if (typeof firebaseNs.firestore !== 'function') {
      throw new Error('Firestore SDK is not loaded.');
    }

    _app = firebaseNs.apps && firebaseNs.apps.length
      ? firebaseNs.app()
      : firebaseNs.initializeApp(config);
    _db = firebaseNs.firestore();
  }

  async function init() {
    if (_initPromise) return _initPromise;

    _initPromise = (async () => {
      _clearLegacyStorage();
      _resetState();

      try {
        await _initFirebase();
        await _loadAllData();
        _setStatus('firebase', null);
        console.log('[AppDB] Initialised in Firebase mode – students:', _students.length,
          '| leaves:', _leaves.length,
          '| tardies:', _tardies.length,
          '| holidays:', _holidays.length);
      } catch (error) {
        _db = null;
        _app = null;
        _setStatus('memory', error && error.message ? error.message : 'Firebase initialisation failed.');
        console.error('[AppDB] Falling back to memory mode:', error);
      }
    })();

    return _initPromise;
  }

  function status() {
    return { ..._status };
  }

  // ---------------------------------------------------------------------------
  // Students
  // ---------------------------------------------------------------------------

  function students() {
    return _students.slice();
  }

  async function addStudent(student) {
    const record = _normalizeStudentRecord(student);
    if (!record) return null;

    _students.push(record);
    await _queueWrite(() => _db.collection(COLLECTIONS.students).doc(record.id).set(_stripUndefined({
      ...record,
      id: undefined,
    })));
    return { ...record };
  }

  async function updateStudent(id, data) {
    const idx = _students.findIndex(student => student.id === id);
    if (idx === -1) return null;

    const updated = _normalizeStudentRecord({
      ..._students[idx],
      ...data,
      id: id,
    });
    _students[idx] = updated;

    await _queueWrite(() => _db.collection(COLLECTIONS.students).doc(id).set(_stripUndefined({
      ...updated,
      id: undefined,
    })));
    return { ...updated };
  }

  async function removeStudent(id) {
    const idx = _students.findIndex(student => student.id === id);
    if (idx === -1) return false;

    _students.splice(idx, 1);
    await _queueWrite(() => _db.collection(COLLECTIONS.students).doc(id).delete());
    return true;
  }

  async function importStudents(rows) {
    let added = 0;
    let updated = 0;
    const changed = [];

    rows.forEach(row => {
      const normalized = _normalizeStudentRecord(row);
      if (!normalized) return;

      const existing = _students.find(student =>
        normalized.studentNo && student.studentNo === normalized.studentNo
      );

      if (existing) {
        const merged = _normalizeStudentRecord({
          ...existing,
          ...normalized,
          id: existing.id,
        });
        Object.assign(existing, merged);
        changed.push({ ...existing });
        updated++;
      } else {
        _students.push(normalized);
        changed.push({ ...normalized });
        added++;
      }
    });

    await _queueWrite(() => _commitChunkedSet(COLLECTIONS.students, changed));
    return { added, updated, total: _students.length };
  }

  function getStudentsByClass(className) {
    return _students
      .filter(student => student.className === className)
      .slice()
      .sort((a, b) => (parseInt(a.seatNo, 10) || 0) - (parseInt(b.seatNo, 10) || 0));
  }

  function getStudentByNo(studentNo) {
    const found = _students.find(student => student.studentNo === studentNo);
    return found ? { ...found } : null;
  }

  function getClassList() {
    return [...new Set(_students.map(student => student.className).filter(Boolean))].sort(_compareClassNames);
  }

  // ---------------------------------------------------------------------------
  // Leaves
  // ---------------------------------------------------------------------------

  function leave() {
    return _leaves.slice();
  }

  async function addLeave(record) {
    const entry = _normalizeLeaveRecord(record);
    if (!entry) return '';

    _leaves.push(entry);
    await _queueWrite(() => _db.collection(COLLECTIONS.leaves).doc(entry.id).set(_stripUndefined({
      ...entry,
      id: undefined,
    })));
    return entry.id;
  }

  async function updateLeave(id, data) {
    const idx = _leaves.findIndex(leaveRecord => leaveRecord.id === id);
    if (idx === -1) return null;

    const updated = _normalizeLeaveRecord({
      ..._leaves[idx],
      ...data,
      id: id,
    });
    _leaves[idx] = updated;

    await _queueWrite(() => _db.collection(COLLECTIONS.leaves).doc(id).set(_stripUndefined({
      ...updated,
      id: undefined,
    })));
    return { ...updated };
  }

  async function removeLeave(id) {
    const idx = _leaves.findIndex(leaveRecord => leaveRecord.id === id);
    if (idx === -1) return false;

    _leaves.splice(idx, 1);
    await _queueWrite(() => _db.collection(COLLECTIONS.leaves).doc(id).delete());
    return true;
  }

  function getLeavesByDate(dateStr) {
    const date = _toDateStr(dateStr);
    return _leaves.filter(leaveRecord => {
      const start = _toDateStr(leaveRecord.startTime);
      const end = _toDateStr(leaveRecord.endTime);
      return date >= start && date <= end;
    }).slice();
  }

  function getLeavesByDateRange(from, to) {
    const startDate = _toDateStr(from);
    const endDate = _toDateStr(to);
    return _leaves.filter(leaveRecord => {
      const leaveStart = _toDateStr(leaveRecord.startTime);
      const leaveEnd = _toDateStr(leaveRecord.endTime);
      return leaveStart <= endDate && leaveEnd >= startDate;
    }).slice();
  }

  function getPendingLeaves() {
    return _leaves.filter(leaveRecord => leaveRecord.status === 'pending').slice();
  }

  // ---------------------------------------------------------------------------
  // Tardies
  // ---------------------------------------------------------------------------

  function tardies() {
    return _tardies.slice();
  }

  async function addTardy(record) {
    const entry = _normalizeTardyRecord(record);
    if (!entry) return null;

    _tardies.push(entry);
    await _queueWrite(() => _db.collection(COLLECTIONS.tardies).doc(entry.id).set(_stripUndefined({
      ...entry,
      id: undefined,
    })));
    return { ...entry };
  }

  function getTardiesByDate(dateStr) {
    const date = _toDateStr(dateStr);
    return _tardies.filter(tardyRecord => _toDateStr(tardyRecord.date) === date).slice();
  }

  function getTardiesByDateRange(from, to) {
    const startDate = _toDateStr(from);
    const endDate = _toDateStr(to);
    return _tardies.filter(tardyRecord => {
      const date = _toDateStr(tardyRecord.date);
      return date >= startDate && date <= endDate;
    }).slice();
  }

  async function clearTardies() {
    const ids = _tardies.map(tardyRecord => tardyRecord.id);
    _tardies = [];
    await _queueWrite(() => _commitChunkedDelete(COLLECTIONS.tardies, ids));
    return true;
  }

  // ---------------------------------------------------------------------------
  // Holidays
  // ---------------------------------------------------------------------------

  function holidays() {
    return _holidays.slice();
  }

  async function addHoliday(holiday) {
    const entry = _normalizeHolidayRecord(holiday);
    if (!entry) return null;

    _holidays.push(entry);
    await _queueWrite(() => _db.collection(COLLECTIONS.holidays).doc(entry.id).set(_stripUndefined({
      ...entry,
      id: undefined,
    })));
    return { ...entry };
  }

  async function removeHoliday(id) {
    const idx = _holidays.findIndex(holiday => holiday.id === id);
    if (idx === -1) return false;

    _holidays.splice(idx, 1);
    await _queueWrite(() => _db.collection(COLLECTIONS.holidays).doc(id).delete());
    return true;
  }

  function isHoliday(dateStr) {
    const date = _toDateStr(dateStr);
    return _holidays.some(holiday => {
      if (holiday.type === 'schoolday') return false; // 補課日 is not a holiday
      const start = _toDateStr(holiday.startDate || holiday.date);
      const end = _toDateStr(holiday.endDate || holiday.startDate || holiday.date);
      return date >= start && date <= end;
    });
  }

  function isSchoolDay(dateStr) {
    const date = _toDateStr(dateStr);
    return _holidays.some(holiday => {
      if (holiday.type !== 'schoolday') return false;
      const start = _toDateStr(holiday.startDate || holiday.date);
      const end = _toDateStr(holiday.endDate || holiday.startDate || holiday.date);
      return date >= start && date <= end;
    });
  }

  function isWeekend(dateStr) {
    const parsed = _asDate(dateStr);
    if (!parsed) return false;
    const day = parsed.getDay();
    return day === 0 || day === 6;
  }

  function isNonSchoolDay(dateStr) {
    // Weekend that is a designated school day (補課日) is NOT a non-school day
    if (isWeekend(dateStr) && isSchoolDay(dateStr)) return false;
    return isWeekend(dateStr) || isHoliday(dateStr);
  }

  // ---------------------------------------------------------------------------
  // Settings
  // ---------------------------------------------------------------------------

  function getSettings() {
    return _cloneSettings();
  }

  async function setSetting(key, value) {
    _settings[key] = value;
    const nextSettings = _cloneSettings();
    await _queueWrite(() => _db.collection(COLLECTIONS.settings).doc(SETTINGS_DOC_ID).set(nextSettings, { merge: true }));
    return _cloneSettings();
  }

  // ---------------------------------------------------------------------------
  // Period Calculation
  // ---------------------------------------------------------------------------

  function calculatePeriods(startTime, endTime) {
    if (!startTime || !endTime) return 0;

    const start = new Date(startTime);
    const end = new Date(endTime);
    if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || end <= start) return 0;

    const startDate = _toDateStr(start);
    const endDate = _toDateStr(end);
    const cursor = new Date(startDate + 'T00:00:00');
    const last = new Date(endDate + 'T00:00:00');
    let totalPeriods = 0;

    while (cursor <= last) {
      const dayStr = _toDateStr(cursor);

      if (!isNonSchoolDay(dayStr)) { // includes 補課日 weekends
        let dayStartMinutes = 0;
        let dayEndMinutes = 24 * 60;

        if (dayStr === startDate) {
          dayStartMinutes = start.getHours() * 60 + start.getMinutes();
        }
        if (dayStr === endDate) {
          dayEndMinutes = end.getHours() * 60 + end.getMinutes();
        }

        ACADEMIC_PERIODS.forEach(period => {
          const periodStart = _timeToMinutes(period.start);
          const periodEnd = _timeToMinutes(period.end);
          if (periodStart < dayEndMinutes && periodEnd > dayStartMinutes) {
            totalPeriods++;
          }
        });
      }

      cursor.setDate(cursor.getDate() + 1);
    }

    return totalPeriods;
  }

  // ---------------------------------------------------------------------------
  // Google Calendar Integration
  // ---------------------------------------------------------------------------

  /**
   * Fetch Taiwan public holidays from Google Calendar API.
   * Uses the public "Taiwan Holidays" calendar.
   * Requires Google Calendar API to be enabled in the Firebase/GCP project.
   *
   * @param {number} [year] - The year to fetch. Defaults to current year.
   * @returns {Promise<Array<{date: string, endDate: string, name: string}>>}
   */
  async function fetchGoogleCalendarHolidays(year) {
    const config = _getFirebaseConfig();
    if (!config || !config.apiKey) {
      throw new Error('Firebase API key is required for Google Calendar integration.');
    }

    const targetYear = year || new Date().getFullYear();
    const calendarId = encodeURIComponent('zh-tw.taiwan#holiday@group.v.calendar.google.com');
    const timeMin = `${targetYear}-01-01T00:00:00Z`;
    const timeMax = `${targetYear}-12-31T23:59:59Z`;

    const url = `https://www.googleapis.com/calendar/v3/calendars/${calendarId}/events`
      + `?key=${config.apiKey}`
      + `&timeMin=${timeMin}`
      + `&timeMax=${timeMax}`
      + `&singleEvents=true`
      + `&orderBy=startTime`
      + `&maxResults=100`;

    const response = await fetch(url);
    if (!response.ok) {
      const errBody = await response.text();
      throw new Error(`Google Calendar API error (${response.status}): ${errBody}`);
    }

    const data = await response.json();
    const items = data.items || [];

    return items.map(event => {
      // Google Calendar all-day events use date (not dateTime)
      const startDate = event.start.date || (event.start.dateTime || '').slice(0, 10);
      // endDate for all-day events is exclusive (next day), so subtract 1 day
      let endDate = startDate;
      if (event.end && event.end.date) {
        const ed = new Date(event.end.date + 'T00:00:00');
        ed.setDate(ed.getDate() - 1);
        endDate = ed.toISOString().slice(0, 10);
      }
      return {
        date: startDate,
        startDate: startDate,
        endDate: endDate,
        name: event.summary || '',
      };
    }).filter(h => h.date);
  }

  /**
   * Import holidays from Google Calendar into the local holiday list.
   * Skips duplicates (same date + name).
   *
   * @param {number} [year] - The year to fetch.
   * @returns {Promise<{added: number, skipped: number, total: number}>}
   */
  async function importGoogleCalendarHolidays(year) {
    const fetched = await fetchGoogleCalendarHolidays(year);
    const existingKeys = new Set(
      _holidays.map(h => (h.startDate || h.date) + '|' + (h.name || ''))
    );

    let added = 0;
    let skipped = 0;

    for (const h of fetched) {
      const key = h.startDate + '|' + h.name;
      if (existingKeys.has(key)) {
        skipped++;
        continue;
      }
      await addHoliday({
        startDate: h.startDate,
        endDate: h.endDate,
        name: h.name,
        type: 'google-calendar',
      });
      existingKeys.add(key);
      added++;
    }

    return { added, skipped, total: _holidays.length };
  }

  // ---------------------------------------------------------------------------
  // Export helpers
  // ---------------------------------------------------------------------------

  function exportLeaves(from, to) {
    return getLeavesByDateRange(from, to).map(leaveRecord => ({
      id: leaveRecord.id,
      studentNo: leaveRecord.studentNo,
      className: leaveRecord.className,
      seatNo: leaveRecord.seatNo,
      chineseName: leaveRecord.chineseName,
      englishName: leaveRecord.englishName,
      leaveType: leaveRecord.leaveType,
      startTime: leaveRecord.startTime,
      endTime: leaveRecord.endTime,
      periods: leaveRecord.periods,
      reason: leaveRecord.reason,
      status: leaveRecord.status,
      createdAt: leaveRecord.createdAt,
    }));
  }

  function exportTardies(from, to) {
    return getTardiesByDateRange(from, to).map(tardyRecord => ({
      id: tardyRecord.id,
      studentNo: tardyRecord.studentNo,
      className: tardyRecord.className,
      seatNo: tardyRecord.seatNo,
      chineseName: tardyRecord.chineseName,
      englishName: tardyRecord.englishName,
      reason: tardyRecord.reason,
      date: tardyRecord.date,
      createdAt: tardyRecord.createdAt,
    }));
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  window.AppDB = {
    PERIODS,

    // Lifecycle
    init,
    status,

    // Students
    students,
    addStudent,
    updateStudent,
    removeStudent,
    importStudents,
    getStudentsByClass,
    getStudentByNo,
    getClassList,
    compareClassNames: _compareClassNames,

    // Leaves
    leave,
    addLeave,
    updateLeave,
    removeLeave,
    getLeavesByDate,
    getLeavesByDateRange,
    getPendingLeaves,

    // Tardies
    tardies,
    addTardy,
    getTardiesByDate,
    getTardiesByDateRange,
    clearTardies,

    // Holidays
    holidays,
    addHoliday,
    removeHoliday,
    isHoliday,
    isSchoolDay,
    isWeekend,
    isNonSchoolDay,

    // Settings
    getSettings,
    setSetting,

    // Calculations
    calculatePeriods,

    // Google Calendar
    fetchGoogleCalendarHolidays,
    importGoogleCalendarHolidays,

    // Export
    exportLeaves,
    exportTardies,
  };
})();
