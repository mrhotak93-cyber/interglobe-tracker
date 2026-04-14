const path = require('path');
const fs = require('fs');
const express = require('express');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const Database = require('better-sqlite3');
const multer = require('multer');

const app = express();
const PORT = process.env.PORT || 3000;

const dataDir = path.join(__dirname, 'data');
const publicDir = path.join(__dirname, 'public');
const uploadsDir = path.join(publicDir, 'uploads');
fs.mkdirSync(dataDir, { recursive: true });
fs.mkdirSync(uploadsDir, { recursive: true });

const db = new Database(path.join(dataDir, 'interglobe.db'));
db.pragma('journal_mode = WAL');

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

app.use('/public', express.static(publicDir));
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(session({
  secret: 'interglobe-secret-key',
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 1000 * 60 * 60 * 8 }
}));

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadsDir),
  filename: (req, file, cb) => {
    const safe = `${Date.now()}-${file.originalname.replace(/[^a-zA-Z0-9.\-_]/g, '_')}`;
    cb(null, safe);
  }
});
const upload = multer({ storage });

function initDb() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      full_name TEXT NOT NULL,
      username TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL CHECK(role IN ('dispatch','driver','client')),
      phone TEXT,
      company TEXT,
      active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      created_by INTEGER
    );

    CREATE TABLE IF NOT EXISTS tours (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      reference TEXT NOT NULL,
      date TEXT NOT NULL,
      driver_id INTEGER,
      client_id INTEGER,
      status TEXT NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','assigned','in_progress','completed')),
      notes TEXT,
      started_at TEXT,
      completed_at TEXT,
      km_total REAL NOT NULL DEFAULT 0,
      created_by INTEGER,
      generated_from_recurrence_id INTEGER,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(driver_id) REFERENCES users(id),
      FOREIGN KEY(client_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS stops (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tour_id INTEGER NOT NULL,
      sequence_no INTEGER NOT NULL,
      stop_type TEXT NOT NULL CHECK(stop_type IN ('loading','unloading')),
      name TEXT NOT NULL,
      street TEXT NOT NULL,
      street_no TEXT,
      postal_code TEXT,
      city TEXT,
      instructions TEXT,
      contact_name TEXT,
      contact_phone TEXT,
      latitude REAL,
      longitude REAL,
      arrived_at TEXT,
      departed_at TEXT,
      status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','arrived','done')),
      FOREIGN KEY(tour_id) REFERENCES tours(id)
    );

    CREATE TABLE IF NOT EXISTS proofs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      stop_id INTEGER NOT NULL,
      image_path TEXT NOT NULL,
      note TEXT,
      uploaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(stop_id) REFERENCES stops(id)
    );

    CREATE TABLE IF NOT EXISTS locations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tour_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      latitude REAL NOT NULL,
      longitude REAL NOT NULL,
      accuracy REAL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(tour_id) REFERENCES tours(id),
      FOREIGN KEY(user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS recurrences (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_tour_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      frequency TEXT NOT NULL CHECK(frequency IN ('daily','weekly')),
      weekdays TEXT,
      start_date TEXT NOT NULL,
      end_date TEXT,
      active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(source_tour_id) REFERENCES tours(id)
    );
  `);

  const count = db.prepare('SELECT COUNT(*) AS count FROM users').get().count;
  if (count === 0) {
    const passwordHash = bcrypt.hashSync('1234', 10);
    const insertUser = db.prepare(`
      INSERT INTO users (full_name, username, password_hash, role, phone, company, active)
      VALUES (?, ?, ?, ?, ?, ?, 1)
    `);
    const dispatch = insertUser.run('Dispatch Demo', 'dispatch', passwordHash, 'dispatch', '0499 00 00 01', 'InterGlobe');
    const driver = insertUser.run('Chauffeur Demo', 'driver', passwordHash, 'driver', '0499 00 00 02', 'InterGlobe');
    const client = insertUser.run('Client Demo', 'client', passwordHash, 'client', '0499 00 00 03', 'Client InterGlobe');

    const tour = db.prepare(`
      INSERT INTO tours (reference, date, driver_id, client_id, status, notes, created_by)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run('T-2026-0001', today(), driver.lastInsertRowid, client.lastInsertRowid, 'assigned', 'Tournée de démonstration', dispatch.lastInsertRowid);

    const insertStop = db.prepare(`
      INSERT INTO stops (
        tour_id, sequence_no, stop_type, name, street, street_no, postal_code, city, instructions, contact_name, contact_phone
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    insertStop.run(tour.lastInsertRowid, 1, 'loading', 'Entrepôt Nord', 'Rue du Port', '12', '1000', 'Bruxelles', 'Se présenter au quai 3', 'Magasinier', '020000001');
    insertStop.run(tour.lastInsertRowid, 2, 'unloading', 'Client Central', 'Avenue Louise', '155', '1050', 'Bruxelles', 'Livrer à la réception', 'Réception', '020000002');
  }
}

function today() {
  return new Date().toISOString().slice(0, 10);
}

function nowIso() {
  return new Date().toISOString();
}

function formatDateTime(value) {
  if (!value) return '';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString('fr-BE');
}

function isSameDay(dateStrA, dateStrB) {
  return dateStrA === dateStrB;
}

function requireAuth(req, res, next) {
  if (!req.session.user) return res.redirect('/login');
  next();
}

function requireRole(...roles) {
  return (req, res, next) => {
    if (!req.session.user) return res.redirect('/login');
    if (!roles.includes(req.session.user.role)) return res.status(403).send('Accès refusé');
    next();
  };
}

app.use((req, res, next) => {
  res.locals.user = req.session.user || null;
  res.locals.flash = req.session.flash || '';
  delete req.session.flash;
  res.locals.formatDateTime = formatDateTime;
  next();
});

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (v) => (v * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function recomputeTourKm(tourId) {
  const locations = db.prepare(`
    SELECT latitude, longitude
    FROM locations
    WHERE tour_id = ?
    ORDER BY created_at ASC, id ASC
  `).all(tourId);

  let km = 0;
  for (let i = 1; i < locations.length; i += 1) {
    km += haversineKm(
      Number(locations[i - 1].latitude),
      Number(locations[i - 1].longitude),
      Number(locations[i].latitude),
      Number(locations[i].longitude)
    );
  }

  db.prepare('UPDATE tours SET km_total = ? WHERE id = ?').run(Number(km.toFixed(2)), tourId);
  return Number(km.toFixed(2));
}

function fullAddress(stop) {
  return [stop.street, stop.street_no, stop.postal_code, stop.city].filter(Boolean).join(', ');
}

function getTourWithStops(tourId) {
  const tour = db.prepare(`
    SELECT t.*,
           d.full_name AS driver_name,
           d.username AS driver_username,
           c.full_name AS client_name,
           c.username AS client_username
    FROM tours t
    LEFT JOIN users d ON d.id = t.driver_id
    LEFT JOIN users c ON c.id = t.client_id
    WHERE t.id = ?
  `).get(tourId);

  if (!tour) return null;

  const stops = db.prepare(`
    SELECT *
    FROM stops
    WHERE tour_id = ?
    ORDER BY sequence_no ASC, id ASC
  `).all(tourId).map((stop) => {
    const proofs = db.prepare(`
      SELECT *
      FROM proofs
      WHERE stop_id = ?
      ORDER BY uploaded_at DESC, id DESC
    `).all(stop.id);
    return { ...stop, proofs, address: fullAddress(stop) };
  });

  const latestLocation = db.prepare(`
    SELECT *
    FROM locations
    WHERE tour_id = ?
    ORDER BY created_at DESC, id DESC
    LIMIT 1
  `).get(tourId);

  return { ...tour, stops, latestLocation };
}

function cloneTour(sourceTourId, targetDate, recurrenceId = null) {
  const sourceTour = db.prepare('SELECT * FROM tours WHERE id = ?').get(sourceTourId);
  if (!sourceTour) throw new Error('Tournée source introuvable');

  if (recurrenceId) {
    const existing = db.prepare(`
      SELECT id FROM tours
      WHERE generated_from_recurrence_id = ? AND date = ?
    `).get(recurrenceId, targetDate);
    if (existing) return null;
  }

  const insertTour = db.prepare(`
    INSERT INTO tours (
      reference, date, driver_id, client_id, status, notes, created_by, generated_from_recurrence_id
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const newRef = `${sourceTour.reference}-${targetDate.replace(/-/g, '')}`;
  const newTour = insertTour.run(
    newRef,
    targetDate,
    sourceTour.driver_id,
    sourceTour.client_id,
    'assigned',
    sourceTour.notes,
    sourceTour.created_by,
    recurrenceId
  );

  const stops = db.prepare(`
    SELECT *
    FROM stops
    WHERE tour_id = ?
    ORDER BY sequence_no ASC, id ASC
  `).all(sourceTourId);

  const insertStop = db.prepare(`
    INSERT INTO stops (
      tour_id, sequence_no, stop_type, name, street, street_no, postal_code, city,
      instructions, contact_name, contact_phone, latitude, longitude, status
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
  `);

  stops.forEach((stop) => {
    insertStop.run(
      newTour.lastInsertRowid,
      stop.sequence_no,
      stop.stop_type,
      stop.name,
      stop.street,
      stop.street_no,
      stop.postal_code,
      stop.city,
      stop.instructions,
      stop.contact_name,
      stop.contact_phone,
      stop.latitude,
      stop.longitude
    );
  });

  return newTour.lastInsertRowid;
}

function listDatesBetween(startDate, endDate) {
  const out = [];
  const start = new Date(`${startDate}T00:00:00`);
  const end = new Date(`${endDate}T00:00:00`);
  while (start <= end) {
    out.push(start.toISOString().slice(0, 10));
    start.setDate(start.getDate() + 1);
  }
  return out;
}

function weekdayMatches(dateStr, weekdaysCsv) {
  if (!weekdaysCsv) return true;
  const jsDay = new Date(`${dateStr}T00:00:00`).getDay(); // 0=Sun
  const map = { 0: 'sun', 1: 'mon', 2: 'tue', 3: 'wed', 4: 'thu', 5: 'fri', 6: 'sat' };
  const selected = weekdaysCsv.split(',').map((v) => v.trim()).filter(Boolean);
  return selected.includes(map[jsDay]);
}

function generateRecurrence(recurrenceId, rangeStart, rangeEnd) {
  const recurrence = db.prepare('SELECT * FROM recurrences WHERE id = ?').get(recurrenceId);
  if (!recurrence || !recurrence.active) return 0;

  const effectiveStart = rangeStart > recurrence.start_date ? rangeStart : recurrence.start_date;
  const effectiveEnd = recurrence.end_date && recurrence.end_date < rangeEnd ? recurrence.end_date : rangeEnd;
  if (effectiveStart > effectiveEnd) return 0;

  let created = 0;
  listDatesBetween(effectiveStart, effectiveEnd).forEach((dateStr) => {
    const shouldGenerate = recurrence.frequency === 'daily'
      ? true
      : weekdayMatches(dateStr, recurrence.weekdays);

    if (shouldGenerate) {
      const result = cloneTour(recurrence.source_tour_id, dateStr, recurrence.id);
      if (result) created += 1;
    }
  });
  return created;
}

initDb();

app.get('/', requireAuth, (req, res) => {
  if (req.session.user.role === 'dispatch') return res.redirect('/dispatch');
  if (req.session.user.role === 'driver') return res.redirect('/driver');
  return res.redirect('/client');
});

app.get('/login', (req, res) => {
  if (req.session.user) return res.redirect('/');
  res.render('login', { title: 'Connexion' });
});

app.post('/login', (req, res) => {
  const { username, password } = req.body;
  const user = db.prepare(`
    SELECT *
    FROM users
    WHERE username = ? AND active = 1
  `).get((username || '').trim());

  if (!user || !bcrypt.compareSync(password || '', user.password_hash)) {
    req.session.flash = 'Identifiants invalides.';
    return res.redirect('/login');
  }

  req.session.user = {
    id: user.id,
    full_name: user.full_name,
    username: user.username,
    role: user.role
  };

  res.redirect('/');
});

app.post('/logout', requireAuth, (req, res) => {
  req.session.destroy(() => res.redirect('/login'));
});

app.get('/dispatch', requireRole('dispatch'), (req, res) => {
  const counts = {
    users: db.prepare('SELECT COUNT(*) AS count FROM users').get().count,
    toursToday: db.prepare('SELECT COUNT(*) AS count FROM tours WHERE date = ?').get(today()).count,
    activeTours: db.prepare("SELECT COUNT(*) AS count FROM tours WHERE status = 'in_progress'").get().count,
    recurrences: db.prepare('SELECT COUNT(*) AS count FROM recurrences WHERE active = 1').get().count
  };

  const tours = db.prepare(`
    SELECT t.*, d.full_name AS driver_name, c.full_name AS client_name
    FROM tours t
    LEFT JOIN users d ON d.id = t.driver_id
    LEFT JOIN users c ON c.id = t.client_id
    ORDER BY t.date DESC, t.id DESC
    LIMIT 8
  `).all();

  res.render('dispatch/dashboard', { title: 'Dispatch', counts, tours });
});

app.get('/dispatch/users', requireRole('dispatch'), (req, res) => {
  const users = db.prepare('SELECT * FROM users ORDER BY role ASC, full_name ASC').all();
  res.render('dispatch/users', { title: 'Utilisateurs', users });
});

app.post('/dispatch/users', requireRole('dispatch'), (req, res) => {
  const {
    full_name, username, password, role, phone, company, active
  } = req.body;

  if (!full_name || !username || !password || !role) {
    req.session.flash = 'Nom, nom d’utilisateur, mot de passe et rôle sont obligatoires.';
    return res.redirect('/dispatch/users');
  }

  try {
    db.prepare(`
      INSERT INTO users (full_name, username, password_hash, role, phone, company, active, created_by)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      full_name.trim(),
      username.trim(),
      bcrypt.hashSync(password, 10),
      role,
      phone || '',
      company || '',
      active ? 1 : 0,
      req.session.user.id
    );
    req.session.flash = 'Utilisateur créé.';
  } catch (error) {
    req.session.flash = `Erreur création utilisateur : ${error.message}`;
  }

  res.redirect('/dispatch/users');
});

app.post('/dispatch/users/:id/toggle', requireRole('dispatch'), (req, res) => {
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(req.params.id);
  if (!user) {
    req.session.flash = 'Utilisateur introuvable.';
    return res.redirect('/dispatch/users');
  }
  db.prepare('UPDATE users SET active = ? WHERE id = ?').run(user.active ? 0 : 1, user.id);
  req.session.flash = 'Statut utilisateur mis à jour.';
  res.redirect('/dispatch/users');
});

app.get('/dispatch/tours', requireRole('dispatch'), (req, res) => {
  const tours = db.prepare(`
    SELECT t.*, d.full_name AS driver_name, c.full_name AS client_name
    FROM tours t
    LEFT JOIN users d ON d.id = t.driver_id
    LEFT JOIN users c ON c.id = t.client_id
    ORDER BY t.date DESC, t.id DESC
  `).all();
  const drivers = db.prepare("SELECT id, full_name FROM users WHERE role = 'driver' AND active = 1 ORDER BY full_name").all();
  const clients = db.prepare("SELECT id, full_name FROM users WHERE role = 'client' AND active = 1 ORDER BY full_name").all();
  res.render('dispatch/tours', { title: 'Tournées', tours, drivers, clients, today });
});

app.post('/dispatch/tours', requireRole('dispatch'), (req, res) => {
  const {
    reference, date, driver_id, client_id, notes
  } = req.body;

  if (!reference || !date) {
    req.session.flash = 'Référence et date obligatoires.';
    return res.redirect('/dispatch/tours');
  }

  db.prepare(`
    INSERT INTO tours (reference, date, driver_id, client_id, status, notes, created_by)
    VALUES (?, ?, ?, ?, 'assigned', ?, ?)
  `).run(
    reference.trim(),
    date,
    driver_id || null,
    client_id || null,
    notes || '',
    req.session.user.id
  );
  req.session.flash = 'Tournée créée.';
  res.redirect('/dispatch/tours');
});

app.get('/dispatch/tours/:id', requireRole('dispatch'), (req, res) => {
  const tour = getTourWithStops(req.params.id);
  if (!tour) return res.status(404).send('Tournée introuvable');
  res.render('dispatch/tour_detail', { title: `Tournée ${tour.reference}`, tour });
});

app.get('/dispatch/tours/:id/edit', requireRole('dispatch'), (req, res) => {
  const tour = db.prepare('SELECT * FROM tours WHERE id = ?').get(req.params.id);
  if (!tour) return res.status(404).send('Tournée introuvable');
  const drivers = db.prepare("SELECT id, full_name FROM users WHERE role = 'driver' AND active = 1 ORDER BY full_name").all();
  const clients = db.prepare("SELECT id, full_name FROM users WHERE role = 'client' AND active = 1 ORDER BY full_name").all();
  res.render('dispatch/tour_form', { title: 'Modifier tournée', tour, drivers, clients });
});

app.post('/dispatch/tours/:id/edit', requireRole('dispatch'), (req, res) => {
  const {
    reference, date, driver_id, client_id, status, notes
  } = req.body;

  db.prepare(`
    UPDATE tours
    SET reference = ?, date = ?, driver_id = ?, client_id = ?, status = ?, notes = ?
    WHERE id = ?
  `).run(reference, date, driver_id || null, client_id || null, status, notes || '', req.params.id);

  req.session.flash = 'Tournée mise à jour.';
  res.redirect(`/dispatch/tours/${req.params.id}`);
});

app.post('/dispatch/tours/:id/stops', requireRole('dispatch'), (req, res) => {
  const {
    sequence_no, stop_type, name, street, street_no, postal_code, city,
    instructions, contact_name, contact_phone
  } = req.body;

  db.prepare(`
    INSERT INTO stops (
      tour_id, sequence_no, stop_type, name, street, street_no, postal_code, city, instructions, contact_name, contact_phone
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    req.params.id,
    Number(sequence_no || 1),
    stop_type,
    name,
    street,
    street_no || '',
    postal_code || '',
    city || '',
    instructions || '',
    contact_name || '',
    contact_phone || ''
  );

  req.session.flash = 'Arrêt ajouté.';
  res.redirect(`/dispatch/tours/${req.params.id}`);
});

app.get('/dispatch/stops/:id/edit', requireRole('dispatch'), (req, res) => {
  const stop = db.prepare('SELECT * FROM stops WHERE id = ?').get(req.params.id);
  if (!stop) return res.status(404).send('Arrêt introuvable');
  res.render('dispatch/stop_form', { title: 'Modifier arrêt', stop });
});

app.post('/dispatch/stops/:id/edit', requireRole('dispatch'), (req, res) => {
  const {
    sequence_no, stop_type, name, street, street_no, postal_code, city,
    instructions, contact_name, contact_phone, latitude, longitude
  } = req.body;
  const stop = db.prepare('SELECT * FROM stops WHERE id = ?').get(req.params.id);
  if (!stop) return res.status(404).send('Arrêt introuvable');

  db.prepare(`
    UPDATE stops
    SET sequence_no = ?, stop_type = ?, name = ?, street = ?, street_no = ?, postal_code = ?, city = ?,
        instructions = ?, contact_name = ?, contact_phone = ?, latitude = ?, longitude = ?
    WHERE id = ?
  `).run(
    Number(sequence_no || 1),
    stop_type,
    name,
    street,
    street_no || '',
    postal_code || '',
    city || '',
    instructions || '',
    contact_name || '',
    contact_phone || '',
    latitude || null,
    longitude || null,
    req.params.id
  );

  req.session.flash = 'Arrêt mis à jour.';
  res.redirect(`/dispatch/tours/${stop.tour_id}`);
});

app.post('/dispatch/stops/:id/delete', requireRole('dispatch'), (req, res) => {
  const stop = db.prepare('SELECT * FROM stops WHERE id = ?').get(req.params.id);
  if (!stop) return res.status(404).send('Arrêt introuvable');
  db.prepare('DELETE FROM proofs WHERE stop_id = ?').run(stop.id);
  db.prepare('DELETE FROM stops WHERE id = ?').run(stop.id);
  req.session.flash = 'Arrêt supprimé.';
  res.redirect(`/dispatch/tours/${stop.tour_id}`);
});

app.post('/dispatch/tours/duplicate-day', requireRole('dispatch'), (req, res) => {
  const { source_date, target_date } = req.body;
  const sourceTours = db.prepare('SELECT id FROM tours WHERE date = ? ORDER BY id').all(source_date);

  let created = 0;
  sourceTours.forEach((tour) => {
    cloneTour(tour.id, target_date, null);
    created += 1;
  });

  req.session.flash = `${created} tournée(s) dupliquée(s) du ${source_date} vers le ${target_date}.`;
  res.redirect('/dispatch/tours');
});

app.get('/dispatch/recurrences', requireRole('dispatch'), (req, res) => {
  const tours = db.prepare(`
    SELECT t.id, t.reference, t.date, d.full_name AS driver_name, c.full_name AS client_name
    FROM tours t
    LEFT JOIN users d ON d.id = t.driver_id
    LEFT JOIN users c ON c.id = t.client_id
    ORDER BY t.date DESC, t.id DESC
  `).all();

  const recurrences = db.prepare(`
    SELECT r.*, t.reference
    FROM recurrences r
    JOIN tours t ON t.id = r.source_tour_id
    ORDER BY r.created_at DESC, r.id DESC
  `).all();

  res.render('dispatch/recurrences', { title: 'Récurrences', tours, recurrences, today });
});

app.post('/dispatch/recurrences', requireRole('dispatch'), (req, res) => {
  const {
    source_tour_id, name, frequency, weekdays, start_date, end_date
  } = req.body;

  db.prepare(`
    INSERT INTO recurrences (source_tour_id, name, frequency, weekdays, start_date, end_date, active)
    VALUES (?, ?, ?, ?, ?, ?, 1)
  `).run(source_tour_id, name, frequency, weekdays || '', start_date, end_date || null);

  req.session.flash = 'Récurrence créée.';
  res.redirect('/dispatch/recurrences');
});

app.post('/dispatch/recurrences/:id/update', requireRole('dispatch'), (req, res) => {
  const {
    name, frequency, weekdays, start_date, end_date, active
  } = req.body;

  db.prepare(`
    UPDATE recurrences
    SET name = ?, frequency = ?, weekdays = ?, start_date = ?, end_date = ?, active = ?
    WHERE id = ?
  `).run(name, frequency, weekdays || '', start_date, end_date || null, active ? 1 : 0, req.params.id);

  req.session.flash = 'Récurrence mise à jour.';
  res.redirect('/dispatch/recurrences');
});

app.post('/dispatch/recurrences/:id/generate', requireRole('dispatch'), (req, res) => {
  const { range_start, range_end } = req.body;
  const created = generateRecurrence(req.params.id, range_start, range_end);
  req.session.flash = `${created} tournée(s) générée(s) depuis la récurrence.`;
  res.redirect('/dispatch/recurrences');
});

app.get('/driver', requireRole('driver'), (req, res) => {
  const tours = db.prepare(`
    SELECT t.*, c.full_name AS client_name
    FROM tours t
    LEFT JOIN users c ON c.id = t.client_id
    WHERE t.driver_id = ?
    ORDER BY t.date DESC, t.id DESC
  `).all(req.session.user.id);
  res.render('driver/dashboard', { title: 'Chauffeur', tours, today });
});

app.get('/driver/tours/:id', requireRole('driver'), (req, res) => {
  const tour = getTourWithStops(req.params.id);
  if (!tour || Number(tour.driver_id) !== Number(req.session.user.id)) {
    return res.status(403).send('Tournée inaccessible');
  }
  res.render('driver/tour', { title: `Ma tournée ${tour.reference}`, tour });
});

app.post('/driver/tours/:id/start', requireRole('driver'), (req, res) => {
  const tour = db.prepare('SELECT * FROM tours WHERE id = ?').get(req.params.id);
  if (!tour || Number(tour.driver_id) !== Number(req.session.user.id)) return res.status(403).send('Accès refusé');

  db.prepare(`
    UPDATE tours
    SET started_at = COALESCE(started_at, ?), status = 'in_progress'
    WHERE id = ?
  `).run(nowIso(), tour.id);

  req.session.flash = 'Tournée démarrée.';
  res.redirect(`/driver/tours/${tour.id}`);
});

app.post('/driver/stops/:id/arrive', requireRole('driver'), (req, res) => {
  const stop = db.prepare(`
    SELECT s.*, t.driver_id, t.id AS tour_id, t.started_at
    FROM stops s
    JOIN tours t ON t.id = s.tour_id
    WHERE s.id = ?
  `).get(req.params.id);

  if (!stop || Number(stop.driver_id) !== Number(req.session.user.id)) return res.status(403).send('Accès refusé');

  db.prepare(`
    UPDATE stops
    SET arrived_at = COALESCE(arrived_at, ?), status = 'arrived'
    WHERE id = ?
  `).run(nowIso(), stop.id);

  db.prepare(`
    UPDATE tours
    SET started_at = COALESCE(started_at, ?), status = 'in_progress'
    WHERE id = ?
  `).run(nowIso(), stop.tour_id);

  req.session.flash = 'Heure d’arrivée enregistrée.';
  res.redirect(`/driver/tours/${stop.tour_id}`);
});

app.post('/driver/stops/:id/depart', requireRole('driver'), (req, res) => {
  const stop = db.prepare(`
    SELECT s.*, t.driver_id, t.id AS tour_id
    FROM stops s
    JOIN tours t ON t.id = s.tour_id
    WHERE s.id = ?
  `).get(req.params.id);

  if (!stop || Number(stop.driver_id) !== Number(req.session.user.id)) return res.status(403).send('Accès refusé');

  db.prepare(`
    UPDATE stops
    SET departed_at = COALESCE(departed_at, ?), status = 'done'
    WHERE id = ?
  `).run(nowIso(), stop.id);

  const remaining = db.prepare(`
    SELECT COUNT(*) AS count
    FROM stops
    WHERE tour_id = ? AND status != 'done'
  `).get(stop.tour_id).count;

  if (remaining === 0) {
    db.prepare(`
      UPDATE tours
      SET completed_at = COALESCE(completed_at, ?), status = 'completed'
      WHERE id = ?
    `).run(nowIso(), stop.tour_id);
    recomputeTourKm(stop.tour_id);
  }

  req.session.flash = 'Heure de départ enregistrée.';
  res.redirect(`/driver/tours/${stop.tour_id}`);
});

app.post('/driver/stops/:id/proofs', requireRole('driver'), upload.array('photos', 5), (req, res) => {
  const stop = db.prepare(`
    SELECT s.*, t.driver_id, t.id AS tour_id
    FROM stops s
    JOIN tours t ON t.id = s.tour_id
    WHERE s.id = ?
  `).get(req.params.id);

  if (!stop || Number(stop.driver_id) !== Number(req.session.user.id)) return res.status(403).send('Accès refusé');

  const insert = db.prepare('INSERT INTO proofs (stop_id, image_path, note) VALUES (?, ?, ?)');
  (req.files || []).forEach((file) => {
    insert.run(stop.id, `/public/uploads/${file.filename}`, req.body.note || '');
  });

  req.session.flash = `${(req.files || []).length} photo(s) ajoutée(s).`;
  res.redirect(`/driver/tours/${stop.tour_id}`);
});

app.post('/driver/tours/:id/location', requireRole('driver'), (req, res) => {
  const tour = db.prepare('SELECT * FROM tours WHERE id = ?').get(req.params.id);
  if (!tour || Number(tour.driver_id) !== Number(req.session.user.id)) {
    return res.status(403).json({ ok: false, error: 'Accès refusé' });
  }

  const { latitude, longitude, accuracy } = req.body;
  if (typeof latitude !== 'number' || typeof longitude !== 'number') {
    return res.status(400).json({ ok: false, error: 'Coordonnées invalides' });
  }

  db.prepare(`
    INSERT INTO locations (tour_id, user_id, latitude, longitude, accuracy)
    VALUES (?, ?, ?, ?, ?)
  `).run(tour.id, req.session.user.id, latitude, longitude, accuracy || null);

  const km = recomputeTourKm(tour.id);
  return res.json({ ok: true, km });
});

app.post('/driver/tours/:id/complete', requireRole('driver'), (req, res) => {
  const tour = db.prepare('SELECT * FROM tours WHERE id = ?').get(req.params.id);
  if (!tour || Number(tour.driver_id) !== Number(req.session.user.id)) return res.status(403).send('Accès refusé');

  recomputeTourKm(tour.id);
  db.prepare(`
    UPDATE tours
    SET completed_at = COALESCE(completed_at, ?), status = 'completed'
    WHERE id = ?
  `).run(nowIso(), tour.id);

  req.session.flash = 'Tournée terminée.';
  res.redirect(`/driver/tours/${tour.id}`);
});

app.get('/client', requireRole('client'), (req, res) => {
  const tours = db.prepare(`
    SELECT t.*, d.full_name AS driver_name
    FROM tours t
    LEFT JOIN users d ON d.id = t.driver_id
    WHERE t.client_id = ?
    ORDER BY t.date DESC, t.id DESC
  `).all(req.session.user.id);
  res.render('client/dashboard', { title: 'Client', tours, today });
});

app.get('/client/tours/:id', requireRole('client'), (req, res) => {
  const tour = getTourWithStops(req.params.id);
  if (!tour || Number(tour.client_id) !== Number(req.session.user.id)) {
    return res.status(403).send('Tournée inaccessible');
  }
  res.render('client/tour', { title: `Suivi ${tour.reference}`, tour });
});

app.get('/api/tours/:id/status', requireAuth, (req, res) => {
  const tour = getTourWithStops(req.params.id);
  if (!tour) return res.status(404).json({ ok: false });

  const user = req.session.user;
  const allowed = user.role === 'dispatch'
    || Number(tour.driver_id) === Number(user.id)
    || Number(tour.client_id) === Number(user.id);

  if (!allowed) return res.status(403).json({ ok: false });

  res.json({
    ok: true,
    tour: {
      id: tour.id,
      status: tour.status,
      started_at: tour.started_at,
      completed_at: tour.completed_at,
      km_total: tour.km_total,
      latestLocation: tour.latestLocation ? {
        latitude: tour.latestLocation.latitude,
        longitude: tour.latestLocation.longitude,
        created_at: tour.latestLocation.created_at,
        maps_link: `https://www.google.com/maps?q=${tour.latestLocation.latitude},${tour.latestLocation.longitude}`
      } : null,
      stops: tour.stops.map((s) => ({
        id: s.id,
        status: s.status,
        arrived_at: s.arrived_at,
        departed_at: s.departed_at
      }))
    }
  });
});

app.use((err, req, res, next) => {
  console.error(err);
  req.session.flash = `Erreur serveur: ${err.message}`;
  res.redirect('back');
});

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`InterGlobe Web MVP lancé sur http://localhost:${PORT}`);
  });
}

module.exports = app;