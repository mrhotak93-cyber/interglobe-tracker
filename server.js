require('dotenv').config();
const path = require('path');
const express = require('express');
const cookieSession = require('cookie-session');
const multer = require('multer');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const PORT = process.env.PORT || 3000;
const publicDir = path.join(__dirname, 'public');

const SUPABASE_URL = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_PUBLISHABLE_KEY =
  process.env.SUPABASE_PUBLISHABLE_KEY || process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY;
const SUPABASE_SECRET_KEY = process.env.SUPABASE_SECRET_KEY;
const SESSION_SECRET = process.env.SESSION_SECRET || 'interglobe-session-secret-change-me';
const PROOF_BUCKET = 'proof-photos';

if (!SUPABASE_URL || !SUPABASE_PUBLISHABLE_KEY || !SUPABASE_SECRET_KEY) {
  console.warn(
    'Variables Supabase manquantes. Ajoute SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY et SUPABASE_SECRET_KEY.'
  );
}

const admin = createClient(
  SUPABASE_URL || 'https://example.supabase.co',
  SUPABASE_SECRET_KEY || 'missing-secret-key',
  {
    auth: { autoRefreshToken: false, persistSession: false }
  }
);

function createAuthClient() {
  return createClient(
    SUPABASE_URL || 'https://example.supabase.co',
    SUPABASE_PUBLISHABLE_KEY || 'missing-publishable-key',
    {
      auth: { autoRefreshToken: false, persistSession: false }
    }
  );
}

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

app.use('/public', express.static(publicDir));
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(
  cookieSession({
    name: 'interglobe_session',
    secret: SESSION_SECRET,
    maxAge: 1000 * 60 * 60 * 8,
    sameSite: 'lax',
    httpOnly: true
  })
);

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }
});

function today() {
  const now = new Date();
  const brussels = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Brussels',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).format(now);

  return brussels;
}
}

function nowIso() {
  return new Date().toISOString();
}

function flash(req, message) {
  req.session.flash = message;
}

function formatDateTime(value) {
  if (!value) return '';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;

  return d.toLocaleString('fr-BE', {
    timeZone: 'Europe/Brussels',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  });
}

function sanitizeFileName(name = 'photo.jpg') {
  return name.replace(/[^a-zA-Z0-9._-]/g, '_');
}

function buildAddress(stop) {
  if (stop.full_address) return stop.full_address;
  return [
    [stop.street, stop.street_number].filter(Boolean).join(' '),
    [stop.postal_code, stop.city].filter(Boolean).join(' ')
  ]
    .filter(Boolean)
    .join(', ');
}

function mapWeekdaysInput(value) {
  if (!value) return [];
  const lookup = { sun: 0, mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6 };
  return String(value)
    .split(',')
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean)
    .map((item) => lookup[item])
    .filter((item) => Number.isInteger(item));
}

function weekdaysToInput(arr = []) {
  const lookup = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'];
  return (arr || [])
    .map((n) => lookup[Number(n)])
    .filter(Boolean)
    .join(',');
}

function addDays(dateStr, days) {
  const d = new Date(`${dateStr}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function dateRange(startDate, endDate) {
  const out = [];
  let cursor = startDate;
  while (cursor <= endDate) {
    out.push(cursor);
    cursor = addDays(cursor, 1);
  }
  return out;
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (v) => (v * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function ensureSupabaseConfigured(req, res, next) {
  if (!SUPABASE_URL || !SUPABASE_PUBLISHABLE_KEY || !SUPABASE_SECRET_KEY) {
    return res.status(500).send('Configuration Supabase incomplète.');
  }
  next();
}

async function fetchProfilesMap() {
  const { data, error } = await admin.from('profiles').select('*');
  if (error) throw error;
  return (data || []).reduce((acc, item) => {
    acc[item.id] = item;
    return acc;
  }, {});
}

async function fetchAllProfiles() {
  const { data, error } = await admin
    .from('profiles')
    .select('*')
    .order('created_at', { ascending: true });
  if (error) throw error;
  return data || [];
}

async function getProfileByAuthUserId(authUserId) {
  const { data, error } = await admin
    .from('profiles')
    .select('*')
    .eq('auth_user_id', authUserId)
    .maybeSingle();
  if (error) throw error;
  return data;
}

async function getProfileByUsername(username) {
  const { data, error } = await admin
    .from('profiles')
    .select('*')
    .ilike('username', username)
    .maybeSingle();
  if (error) throw error;
  return data;
}

function normalizeTour(row, profilesMap) {
  const driver = profilesMap[row.assigned_driver_profile_id] || null;
  const client = profilesMap[row.client_profile_id] || null;

  return {
    ...row,
    reference: row.reference_code,
    date: row.tour_date,
    driver_id: row.assigned_driver_profile_id,
    client_id: row.client_profile_id,
    driver_name: driver ? driver.full_name : null,
    client_name: client ? client.full_name : null,
    started_at: row.started_at,
    completed_at: row.ended_at,
    km_total: Number(row.total_km || 0)
  };
}

function normalizeStop(row) {
  return {
    ...row,
    stop_type: row.stop_kind,
    street_no: row.street_number,
    address: buildAddress(row),
    proofs: []
  };
}

async function fetchTours(where = {}) {
  let query = admin
    .from('tours')
    .select('*')
    .order('tour_date', { ascending: false })
    .order('created_at', { ascending: false });

  Object.entries(where).forEach(([key, value]) => {
    if (Array.isArray(value)) query = query.in(key, value);
    else query = query.eq(key, value);
  });

  const { data, error } = await query;
  if (error) throw error;

  const profilesMap = await fetchProfilesMap();
  return (data || []).map((row) => normalizeTour(row, profilesMap));
}

async function fetchTourById(tourId) {
  const { data, error } = await admin.from('tours').select('*').eq('id', tourId).maybeSingle();
  if (error) throw error;
  if (!data) return null;

  const profilesMap = await fetchProfilesMap();
  const tour = normalizeTour(data, profilesMap);

  const { data: stopsRaw, error: stopsError } = await admin
    .from('tour_stops')
    .select('*')
    .eq('tour_id', tourId)
    .order('sequence_no', { ascending: true });
  if (stopsError) throw stopsError;

  const stops = (stopsRaw || []).map(normalizeStop);
  const stopIds = stops.map((s) => s.id);

  const proofsByStop = {};
  if (stopIds.length) {
    const { data: proofsRaw, error: proofsError } = await admin
      .from('proof_photos')
      .select('*')
      .in('stop_id', stopIds)
      .order('taken_at', { ascending: false });
    if (proofsError) throw proofsError;

    (proofsRaw || []).forEach((proof) => {
      if (!proofsByStop[proof.stop_id]) proofsByStop[proof.stop_id] = [];
      proofsByStop[proof.stop_id].push({
        ...proof,
        image_path: `/media/proofs/${proof.id}`
      });
    });
  }

  stops.forEach((stop) => {
    stop.proofs = proofsByStop[stop.id] || [];
  });

  const { data: latestLocation, error: locationError } = await admin
    .from('gps_tracking_points')
    .select('*')
    .eq('tour_id', tourId)
    .order('tracked_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (locationError) throw locationError;

  tour.stops = stops;
  tour.latestLocation = latestLocation
    ? {
        ...latestLocation,
        created_at: latestLocation.tracked_at,
        maps_link: `https://www.google.com/maps?q=${latestLocation.latitude},${latestLocation.longitude}`
      }
    : null;

  return tour;
}

async function createClientReport(tourId) {
  const tour = await fetchTourById(tourId);
  if (!tour) return;

  const summaryText = [
    `Début : ${formatDateTime(tour.started_at) || '-'}`,
    `Fin : ${formatDateTime(tour.completed_at) || '-'}`,
    `Km : ${tour.km_total || 0}`
  ].join(' | ');

  const payload = {
    tour_id: tourId,
    started_at: tour.started_at,
    ended_at: tour.completed_at,
    total_km: tour.km_total || 0,
    summary_text: summaryText,
    updated_at: nowIso()
  };

  const { error } = await admin.from('client_reports').upsert(payload, { onConflict: 'tour_id' });
  if (error) throw error;
}

async function cloneTourWithStops(sourceTour, targetDate, createdByProfileId, templateId = null) {
  const insertTourPayload = {
    template_id: templateId,
    reference_code: `${sourceTour.reference}${targetDate !== sourceTour.date ? `-${targetDate}` : ''}`,
    title: sourceTour.title || sourceTour.reference,
    tour_date: targetDate,
    client_profile_id: sourceTour.client_id || null,
    assigned_driver_profile_id: sourceTour.driver_id || null,
    status: 'assigned',
    notes: sourceTour.notes || null,
    created_by_profile_id: createdByProfileId || null
  };

  const { data: createdTour, error: createTourError } = await admin
    .from('tours')
    .insert(insertTourPayload)
    .select('*')
    .single();
  if (createTourError) throw createTourError;

  if (sourceTour.stops && sourceTour.stops.length) {
    const stopPayloads = sourceTour.stops.map((stop) => ({
      tour_id: createdTour.id,
      sequence_no: stop.sequence_no,
      stop_kind: stop.stop_type,
      status: 'pending',
      name: stop.name,
      street: stop.street || null,
      street_number: stop.street_no || stop.street_number || null,
      postal_code: stop.postal_code || null,
      city: stop.city || null,
      full_address: stop.address || buildAddress(stop),
      instructions: stop.instructions || null,
      contact_name: stop.contact_name || null,
      contact_phone: stop.contact_phone || null,
      latitude: stop.latitude || null,
      longitude: stop.longitude || null
    }));

    const { error: stopError } = await admin.from('tour_stops').insert(stopPayloads);
    if (stopError) throw stopError;
  }

  return createdTour;
}

async function createTemplateFromTour(sourceTourId, form, createdByProfileId) {
  const sourceTour = await fetchTourById(sourceTourId);
  if (!sourceTour) throw new Error('Tournée source introuvable');

  const recurrence = form.frequency === 'weekly' ? 'weekly' : 'daily';
  const recurrenceWeekdays = recurrence === 'weekly' ? mapWeekdaysInput(form.weekdays) : [];

  const { data: template, error: templateError } = await admin
    .from('route_templates')
    .insert({
      name: form.name,
      description: sourceTour.reference,
      client_profile_id: sourceTour.client_id || null,
      assigned_driver_profile_id: sourceTour.driver_id || null,
      recurrence,
      recurrence_weekdays: recurrenceWeekdays,
      recurrence_start_date: form.start_date,
      recurrence_end_date: form.end_date || null,
      is_active: true,
      created_by_profile_id: createdByProfileId || null
    })
    .select('*')
    .single();
  if (templateError) throw templateError;

  if (sourceTour.stops.length) {
    const payloads = sourceTour.stops.map((stop) => ({
      template_id: template.id,
      sequence_no: stop.sequence_no,
      stop_kind: stop.stop_type,
      name: stop.name,
      street: stop.street || null,
      street_number: stop.street_no || stop.street_number || null,
      postal_code: stop.postal_code || null,
      city: stop.city || null,
      full_address: stop.address,
      instructions: stop.instructions || null,
      contact_name: stop.contact_name || null,
      contact_phone: stop.contact_phone || null,
      latitude: stop.latitude || null,
      longitude: stop.longitude || null
    }));

    const { error: stopError } = await admin.from('route_template_stops').insert(payloads);
    if (stopError) throw stopError;
  }

  return template;
}

function templateMatchesDate(template, dateStr) {
  if (!template.is_active) return false;
  if (template.recurrence_start_date && dateStr < template.recurrence_start_date) return false;
  if (template.recurrence_end_date && dateStr > template.recurrence_end_date) return false;
  if (template.recurrence === 'daily') return true;

  if (template.recurrence === 'weekly') {
    const d = new Date(`${dateStr}T00:00:00Z`);
    const weekday = d.getUTCDay();
    const allowed = template.recurrence_weekdays || [];
    return allowed.length ? allowed.includes(weekday) : true;
  }

  return false;
}

async function generateToursFromTemplate(templateId, rangeStart, rangeEnd, createdByProfileId) {
  const { data: template, error: templateError } = await admin
    .from('route_templates')
    .select('*')
    .eq('id', templateId)
    .single();
  if (templateError) throw templateError;

  const { data: stopsRaw, error: stopsError } = await admin
    .from('route_template_stops')
    .select('*')
    .eq('template_id', templateId)
    .order('sequence_no', { ascending: true });
  if (stopsError) throw stopsError;

  const generatedDates = dateRange(rangeStart, rangeEnd).filter((dateStr) =>
    templateMatchesDate(template, dateStr)
  );

  let createdCount = 0;

  for (const dateStr of generatedDates) {
    const { data: existing, error: existingError } = await admin
      .from('tours')
      .select('id')
      .eq('template_id', template.id)
      .eq('tour_date', dateStr)
      .limit(1)
      .maybeSingle();
    if (existingError) throw existingError;
    if (existing) continue;

    const { data: newTour, error: newTourError } = await admin
      .from('tours')
      .insert({
        template_id: template.id,
        reference_code: `${template.name}-${dateStr}`,
        title: template.name,
        tour_date: dateStr,
        client_profile_id: template.client_profile_id || null,
        assigned_driver_profile_id: template.assigned_driver_profile_id || null,
        status: 'assigned',
        notes: `Généré depuis récurrence ${template.name}`,
        created_by_profile_id: createdByProfileId || null
      })
      .select('*')
      .single();
    if (newTourError) throw newTourError;

    if (stopsRaw && stopsRaw.length) {
      const payloads = stopsRaw.map((stop) => ({
        tour_id: newTour.id,
        sequence_no: stop.sequence_no,
        stop_kind: stop.stop_kind,
        status: 'pending',
        name: stop.name,
        street: stop.street || null,
        street_number: stop.street_number || null,
        postal_code: stop.postal_code || null,
        city: stop.city || null,
        full_address: stop.full_address || buildAddress(stop),
        instructions: stop.instructions || null,
        contact_name: stop.contact_name || null,
        contact_phone: stop.contact_phone || null,
        latitude: stop.latitude || null,
        longitude: stop.longitude || null
      }));

      const { error: insertStopsError } = await admin.from('tour_stops').insert(payloads);
      if (insertStopsError) throw insertStopsError;
    }

    createdCount += 1;
  }

  return createdCount;
}

app.use(ensureSupabaseConfigured);

app.use((req, res, next) => {
  res.locals.user = req.session.user || null;
  res.locals.flash = req.session.flash || '';
  delete req.session.flash;
  res.locals.formatDateTime = formatDateTime;
  res.locals.today = today;
  next();
});

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

app.get('/', (req, res) => {
  if (!req.session.user) return res.redirect('/login');
  if (req.session.user.role === 'dispatch') return res.redirect('/dispatch');
  if (req.session.user.role === 'driver') return res.redirect('/driver');
  if (req.session.user.role === 'client') return res.redirect('/client');
  return res.redirect('/login');
});

app.get('/login', (req, res) => {
  if (req.session.user) return res.redirect('/');
  res.render('login', { title: 'Connexion' });
});

app.post('/login', async (req, res) => {
  try {
    const username = String(req.body.username || '').trim();
    const password = String(req.body.password || '');

    console.log('[LOGIN] username reçu =', username);

    if (!username || !password) {
      flash(req, 'Nom d’utilisateur et mot de passe requis.');
      return res.redirect('/login');
    }

    const profile = await getProfileByUsername(username);
    console.log(
      '[LOGIN] profile trouvé =',
      profile
        ? {
            id: profile.id,
            username: profile.username,
            role: profile.role,
            auth_user_id: profile.auth_user_id,
            is_active: profile.is_active
          }
        : null
    );

    if (!profile || !profile.is_active || !profile.auth_user_id) {
      flash(req, 'Compte introuvable ou inactif.');
      return res.redirect('/login');
    }

    const { data: authInfo, error: authInfoError } = await admin.auth.admin.getUserById(
      profile.auth_user_id
    );

    if (authInfoError) {
      console.error('[LOGIN] getUserById error =', authInfoError);
      flash(req, 'Erreur getUserById.');
      return res.redirect('/login');
    }

    console.log('[LOGIN] email auth =', authInfo?.user?.email || null);

    if (!authInfo?.user?.email) {
      flash(req, 'Email auth introuvable.');
      return res.redirect('/login');
    }

    const authClient = createAuthClient();
    const { data: signInData, error: signInError } = await authClient.auth.signInWithPassword({
      email: authInfo.user.email,
      password
    });

    if (signInError) {
      console.error('[LOGIN] signInWithPassword error =', signInError);
      flash(req, `Erreur login Supabase: ${signInError.message}`);
      return res.redirect('/login');
    }

    if (!signInData?.user) {
      flash(req, 'Utilisateur non retourné par Supabase.');
      return res.redirect('/login');
    }

    req.session.user = {
      profile_id: profile.id,
      auth_user_id: profile.auth_user_id,
      username: profile.username,
      full_name: profile.full_name,
      role: profile.role,
      email: authInfo.user.email
    };

    console.log('[LOGIN] succès pour', username);
    return res.redirect('/');
  } catch (error) {
    console.error('[LOGIN] catch fatal =', error);
    flash(req, `Erreur de connexion: ${error.message}`);
    return res.redirect('/login');
  }
});

app.post('/logout', (req, res) => {
  req.session = null;
  res.redirect('/login');
});

app.get('/dispatch', requireRole('dispatch'), async (req, res) => {
  try {
    const users = await fetchAllProfiles();
    const tours = await fetchTours();
    const { data: templates, error: templateError } = await admin
      .from('route_templates')
      .select('id');
    if (templateError) throw templateError;

    res.render('dispatch/dashboard', {
      title: 'Dashboard dispatch',
      counts: {
        users: users.length,
        toursToday: tours.filter((item) => item.date === today()).length,
        activeTours: tours.filter((item) => item.status === 'in_progress').length,
        recurrences: (templates || []).length
      },
      tours: tours.slice(0, 8)
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur dashboard dispatch');
  }
});

app.get('/dispatch/users', requireRole('dispatch'), async (req, res) => {
  try {
    const users = await fetchAllProfiles();
    res.render('dispatch/users', { title: 'Utilisateurs', users });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur utilisateurs');
  }
});

app.post('/dispatch/users', requireRole('dispatch'), async (req, res) => {
  try {
    const username = String(req.body.username || '').trim();
    const fullName = String(req.body.full_name || '').trim();
    const password = String(req.body.password || '');
    const role = String(req.body.role || 'driver');
    const phone = String(req.body.phone || '').trim() || null;
    const company = String(req.body.company || 'InterGlobe').trim() || 'InterGlobe';
    const isActive = Boolean(req.body.active);

    if (!username || !fullName || !password || !role) {
      flash(req, 'Tous les champs requis doivent être remplis.');
      return res.redirect('/dispatch/users');
    }

    const email = `${username.toLowerCase()}@interglobe-tracker.local`;
    const { data: createdUser, error: createUserError } = await admin.auth.admin.createUser({
      email,
      password,
      email_confirm: true,
      user_metadata: { username, full_name: fullName, role }
    });
    if (createUserError) throw createUserError;

    const { error: profileError } = await admin.from('profiles').insert({
      auth_user_id: createdUser.user.id,
      username,
      full_name: fullName,
      role,
      phone,
      company,
      is_active: isActive,
      must_change_password: false,
      created_by_profile_id: req.session.user.profile_id
    });
    if (profileError) throw profileError;

    flash(req, 'Utilisateur créé.');
    res.redirect('/dispatch/users');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur création utilisateur : ${error.message}`);
    res.redirect('/dispatch/users');
  }
});

app.post('/dispatch/users/:id/toggle', requireRole('dispatch'), async (req, res) => {
  try {
    const { data: existing, error: existingError } = await admin
      .from('profiles')
      .select('id, is_active')
      .eq('id', req.params.id)
      .single();
    if (existingError) throw existingError;

    const { error } = await admin
      .from('profiles')
      .update({ is_active: !existing.is_active })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Statut du compte mis à jour.');
    res.redirect('/dispatch/users');
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur modification utilisateur.');
    res.redirect('/dispatch/users');
  }
});

app.post('/dispatch/users/:id/delete', requireRole('dispatch'), async (req, res) => {
  try {
    const profileId = req.params.id;

    const { data: profile, error: profileFetchError } = await admin
      .from('profiles')
      .select('*')
      .eq('id', profileId)
      .single();
    if (profileFetchError) throw profileFetchError;

    if (profile.username === 'dispatch') {
      flash(req, 'Le compte dispatch principal ne peut pas être supprimé.');
      return res.redirect('/dispatch/users');
    }

    const { error: toursDriverError } = await admin
      .from('tours')
      .update({ assigned_driver_profile_id: null })
      .eq('assigned_driver_profile_id', profileId);
    if (toursDriverError) throw toursDriverError;

    const { error: toursClientError } = await admin
      .from('tours')
      .update({ client_profile_id: null })
      .eq('client_profile_id', profileId);
    if (toursClientError) throw toursClientError;

    const { error: proofError } = await admin
      .from('proof_photos')
      .delete()
      .eq('driver_profile_id', profileId);
    if (proofError) throw proofError;

    const { error: gpsError } = await admin
      .from('gps_tracking_points')
      .delete()
      .eq('driver_profile_id', profileId);
    if (gpsError) throw gpsError;

    const { error: profileDeleteError } = await admin
      .from('profiles')
      .delete()
      .eq('id', profileId);
    if (profileDeleteError) throw profileDeleteError;

    if (profile.auth_user_id) {
      const { error: authDeleteError } = await admin.auth.admin.deleteUser(profile.auth_user_id);
      if (authDeleteError) throw authDeleteError;
    }

    flash(req, 'Utilisateur supprimé.');
    res.redirect('/dispatch/users');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur suppression utilisateur : ${error.message}`);
    res.redirect('/dispatch/users');
  }
});

app.post('/dispatch/tours/:id/delete', requireRole('dispatch'), async (req, res) => {
  try {
    const tourId = req.params.id;

    const { error: proofsError } = await admin
      .from('proof_photos')
      .delete()
      .eq('tour_id', tourId);
    if (proofsError) throw proofsError;

    const { error: gpsError } = await admin
      .from('gps_tracking_points')
      .delete()
      .eq('tour_id', tourId);
    if (gpsError) throw gpsError;

    const { error: reportsError } = await admin
      .from('client_reports')
      .delete()
      .eq('tour_id', tourId);
    if (reportsError) throw reportsError;

    const { error: stopsError } = await admin
      .from('tour_stops')
      .delete()
      .eq('tour_id', tourId);
    if (stopsError) throw stopsError;

    const { error: tourError } = await admin
      .from('tours')
      .delete()
      .eq('id', tourId);
    if (tourError) throw tourError;

    flash(req, 'Tournée supprimée.');
    res.redirect('/dispatch/tours');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur suppression tournée : ${error.message}`);
    res.redirect('/dispatch/tours');
  }
});

app.get('/dispatch/tours', requireRole('dispatch'), async (req, res) => {
  try {
    const tours = await fetchTours();
    const profiles = await fetchAllProfiles();
    const drivers = profiles.filter((item) => item.role === 'driver' && item.is_active);
    const clients = profiles.filter((item) => item.role === 'client' && item.is_active);

    res.render('dispatch/tours', {
      title: 'Tournées',
      tours,
      drivers,
      clients,
      today
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur tournées');
  }
});

app.post('/dispatch/tours', requireRole('dispatch'), async (req, res) => {
  try {
    const payload = {
      reference_code: String(req.body.reference || '').trim(),
      title: String(req.body.reference || '').trim() || 'Tournée',
      tour_date: req.body.date,
      assigned_driver_profile_id: req.body.driver_id || null,
      client_profile_id: req.body.client_id || null,
      status: 'assigned',
      notes: String(req.body.notes || '').trim() || null,
      created_by_profile_id: req.session.user.profile_id
    };

    const { error } = await admin.from('tours').insert(payload);
    if (error) throw error;

    flash(req, 'Tournée créée.');
    res.redirect('/dispatch/tours');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur création tournée : ${error.message}`);
    res.redirect('/dispatch/tours');
  }
});

app.post('/dispatch/tours/duplicate-day', requireRole('dispatch'), async (req, res) => {
  try {
    const sourceDate = req.body.source_date;
    const targetDate = req.body.target_date;
    const sourceTours = await fetchTours({ tour_date: sourceDate });

    if (!sourceTours.length) {
      flash(req, 'Aucune tournée trouvée pour le jour source.');
      return res.redirect('/dispatch/tours');
    }

    for (const tour of sourceTours) {
      const fullTour = await fetchTourById(tour.id);
      await cloneTourWithStops(fullTour, targetDate, req.session.user.profile_id);
    }

    flash(req, 'Planning dupliqué.');
    return res.redirect('/dispatch/tours');
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur duplication planning.');
    return res.redirect('/dispatch/tours');
  }
});

app.get('/dispatch/tours/:id', requireRole('dispatch'), async (req, res) => {
  try {
    const tour = await fetchTourById(req.params.id);
    if (!tour) return res.status(404).send('Tournée introuvable');
    res.render('dispatch/tour_detail', { title: tour.reference, tour });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur détail tournée');
  }
});

app.get('/dispatch/tours/:id/edit', requireRole('dispatch'), async (req, res) => {
  try {
    const tour = await fetchTourById(req.params.id);
    if (!tour) return res.status(404).send('Tournée introuvable');

    const profiles = await fetchAllProfiles();
    const drivers = profiles.filter((item) => item.role === 'driver');
    const clients = profiles.filter((item) => item.role === 'client');

    res.render('dispatch/tour_form', {
      title: 'Modifier tournée',
      tour,
      drivers,
      clients
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur édition tournée');
  }
});

app.post('/dispatch/tours/:id/edit', requireRole('dispatch'), async (req, res) => {
  try {
    const { error } = await admin
      .from('tours')
      .update({
        reference_code: String(req.body.reference || '').trim(),
        title: String(req.body.reference || '').trim() || 'Tournée',
        tour_date: req.body.date,
        assigned_driver_profile_id: req.body.driver_id || null,
        client_profile_id: req.body.client_id || null,
        status: req.body.status || 'draft',
        notes: String(req.body.notes || '').trim() || null,
        ended_at: req.body.status === 'completed' ? nowIso() : null
      })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Tournée mise à jour.');
    res.redirect(`/dispatch/tours/${req.params.id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur modification tournée.');
    res.redirect(`/dispatch/tours/${req.params.id}/edit`);
  }
});

app.post('/dispatch/tours/:id/stops', requireRole('dispatch'), async (req, res) => {
  try {
    const payload = {
      tour_id: req.params.id,
      sequence_no: Number(req.body.sequence_no || 1),
      stop_kind: req.body.stop_type,
      status: 'pending',
      name: String(req.body.name || '').trim(),
      street: String(req.body.street || '').trim() || null,
      street_number: String(req.body.street_no || '').trim() || null,
      postal_code: String(req.body.postal_code || '').trim() || null,
      city: String(req.body.city || '').trim() || null,
      full_address: [
        [String(req.body.street || '').trim(), String(req.body.street_no || '').trim()]
          .filter(Boolean)
          .join(' '),
        [String(req.body.postal_code || '').trim(), String(req.body.city || '').trim()]
          .filter(Boolean)
          .join(' ')
      ]
        .filter(Boolean)
        .join(', '),
      instructions: String(req.body.instructions || '').trim() || null,
      contact_name: String(req.body.contact_name || '').trim() || null,
      contact_phone: String(req.body.contact_phone || '').trim() || null,
      latitude: req.body.latitude || null,
      longitude: req.body.longitude || null
    };

    const { error } = await admin.from('tour_stops').insert(payload);
    if (error) throw error;

    flash(req, 'Arrêt ajouté.');
    res.redirect(`/dispatch/tours/${req.params.id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur ajout arrêt.');
    res.redirect(`/dispatch/tours/${req.params.id}`);
  }
});

app.get('/dispatch/stops/:id/edit', requireRole('dispatch'), async (req, res) => {
  try {
    const { data: stop, error } = await admin
      .from('tour_stops')
      .select('*')
      .eq('id', req.params.id)
      .single();
    if (error) throw error;

    res.render('dispatch/stop_form', {
      title: 'Modifier arrêt',
      stop: normalizeStop(stop)
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur édition arrêt');
  }
});

app.post('/dispatch/stops/:id/edit', requireRole('dispatch'), async (req, res) => {
  try {
    const update = {
      sequence_no: Number(req.body.sequence_no || 1),
      stop_kind: req.body.stop_type,
      name: String(req.body.name || '').trim(),
      street: String(req.body.street || '').trim() || null,
      street_number: String(req.body.street_no || '').trim() || null,
      postal_code: String(req.body.postal_code || '').trim() || null,
      city: String(req.body.city || '').trim() || null,
      full_address: [
        [String(req.body.street || '').trim(), String(req.body.street_no || '').trim()]
          .filter(Boolean)
          .join(' '),
        [String(req.body.postal_code || '').trim(), String(req.body.city || '').trim()]
          .filter(Boolean)
          .join(' ')
      ]
        .filter(Boolean)
        .join(', '),
      instructions: String(req.body.instructions || '').trim() || null,
      contact_name: String(req.body.contact_name || '').trim() || null,
      contact_phone: String(req.body.contact_phone || '').trim() || null,
      latitude: req.body.latitude || null,
      longitude: req.body.longitude || null
    };

    const { data: stop, error: fetchError } = await admin
      .from('tour_stops')
      .select('tour_id')
      .eq('id', req.params.id)
      .single();
    if (fetchError) throw fetchError;

    const { error } = await admin.from('tour_stops').update(update).eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Arrêt mis à jour.');
    res.redirect(`/dispatch/tours/${stop.tour_id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur modification arrêt.');
    res.redirect('/dispatch/tours');
  }
});

app.post('/dispatch/stops/:id/delete', requireRole('dispatch'), async (req, res) => {
  try {
    const { data: stop, error: fetchError } = await admin
      .from('tour_stops')
      .select('tour_id')
      .eq('id', req.params.id)
      .single();
    if (fetchError) throw fetchError;

    const { error } = await admin.from('tour_stops').delete().eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Arrêt supprimé.');
    res.redirect(`/dispatch/tours/${stop.tour_id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur suppression arrêt.');
    res.redirect('/dispatch/tours');
  }
});

app.get('/dispatch/recurrences', requireRole('dispatch'), async (req, res) => {
  try {
    const tours = await fetchTours();
    const { data: templatesRaw, error } = await admin
      .from('route_templates')
      .select('*')
      .order('created_at', { ascending: false });
    if (error) throw error;

    const recurrences = (templatesRaw || []).map((item) => ({
      id: item.id,
      name: item.name,
      reference: item.description || '-',
      frequency: item.recurrence,
      weekdays: weekdaysToInput(item.recurrence_weekdays),
      start_date: item.recurrence_start_date,
      end_date: item.recurrence_end_date,
      active: item.is_active
    }));

    res.render('dispatch/recurrences', {
      title: 'Récurrences',
      tours,
      recurrences,
      today
    });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur récurrences');
  }
});

app.post('/dispatch/recurrences', requireRole('dispatch'), async (req, res) => {
  try {
    await createTemplateFromTour(req.body.source_tour_id, req.body, req.session.user.profile_id);
    flash(req, 'Récurrence créée.');
    res.redirect('/dispatch/recurrences');
  } catch (error) {
    console.error(error);
    flash(req, `Erreur création récurrence : ${error.message}`);
    res.redirect('/dispatch/recurrences');
  }
});

app.post('/dispatch/recurrences/:id/update', requireRole('dispatch'), async (req, res) => {
  try {
    const recurrence = req.body.frequency === 'weekly' ? 'weekly' : 'daily';

    const { error } = await admin
      .from('route_templates')
      .update({
        name: req.body.name,
        recurrence,
        recurrence_weekdays: recurrence === 'weekly' ? mapWeekdaysInput(req.body.weekdays) : [],
        recurrence_start_date: req.body.start_date,
        recurrence_end_date: req.body.end_date || null,
        is_active: Boolean(req.body.active)
      })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Récurrence mise à jour.');
    res.redirect('/dispatch/recurrences');
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur mise à jour récurrence.');
    res.redirect('/dispatch/recurrences');
  }
});

app.post('/dispatch/recurrences/:id/generate', requireRole('dispatch'), async (req, res) => {
  try {
    const count = await generateToursFromTemplate(
      req.params.id,
      req.body.range_start,
      req.body.range_end,
      req.session.user.profile_id
    );

    flash(req, `${count} tournée(s) générée(s).`);
    res.redirect('/dispatch/recurrences');
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur génération récurrence.');
    res.redirect('/dispatch/recurrences');
  }
});

app.get('/driver', requireRole('driver'), async (req, res) => {
  try {
    const tours = await fetchTours({ assigned_driver_profile_id: req.session.user.profile_id });
    res.render('driver/dashboard', { title: 'Mes tournées', tours });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur tournée chauffeur');
  }
});

app.get('/driver/tours/:id', requireRole('driver'), async (req, res) => {
  try {
    const tour = await fetchTourById(req.params.id);
    if (!tour || tour.driver_id !== req.session.user.profile_id) {
      return res.status(404).send('Tournée introuvable');
    }

    res.render('driver/tour', { title: tour.reference, tour });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur détail tournée chauffeur');
  }
});

app.post('/driver/tours/:id/start', requireRole('driver'), async (req, res) => {
  try {
    const { error } = await admin
      .from('tours')
      .update({
        status: 'in_progress',
        started_at: nowIso(),
        ended_at: null
      })
      .eq('id', req.params.id)
      .eq('assigned_driver_profile_id', req.session.user.profile_id);
    if (error) throw error;

    flash(req, 'Tournée démarrée.');
    res.redirect(`/driver/tours/${req.params.id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur démarrage tournée.');
    res.redirect(`/driver/tours/${req.params.id}`);
  }
});

app.post('/driver/tours/:id/complete', requireRole('driver'), async (req, res) => {
  try {
    const endedAt = nowIso();

    const { error } = await admin
      .from('tours')
      .update({
        status: 'completed',
        ended_at: endedAt
      })
      .eq('id', req.params.id)
      .eq('assigned_driver_profile_id', req.session.user.profile_id);
    if (error) throw error;

    await createClientReport(req.params.id);

    flash(req, 'Tournée terminée.');
    res.redirect(`/driver/tours/${req.params.id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur fin tournée.');
    res.redirect(`/driver/tours/${req.params.id}`);
  }
});

app.post('/driver/stops/:id/arrive', requireRole('driver'), async (req, res) => {
  try {
    const { data: stop, error: fetchError } = await admin
      .from('tour_stops')
      .select('id, tour_id')
      .eq('id', req.params.id)
      .single();
    if (fetchError) throw fetchError;

    const { error } = await admin
      .from('tour_stops')
      .update({ status: 'arrived', arrived_at: nowIso() })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Arrivée enregistrée.');
    res.redirect(`/driver/tours/${stop.tour_id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur arrivée arrêt.');
    res.redirect('/driver');
  }
});

app.post('/driver/stops/:id/depart', requireRole('driver'), async (req, res) => {
  try {
    const { data: stop, error: fetchError } = await admin
      .from('tour_stops')
      .select('id, tour_id')
      .eq('id', req.params.id)
      .single();
    if (fetchError) throw fetchError;

    const { error } = await admin
      .from('tour_stops')
      .update({ status: 'done', departed_at: nowIso() })
      .eq('id', req.params.id);
    if (error) throw error;

    flash(req, 'Départ enregistré.');
    res.redirect(`/driver/tours/${stop.tour_id}`);
  } catch (error) {
    console.error(error);
    flash(req, 'Erreur départ arrêt.');
    res.redirect('/driver');
  }
});

app.post('/driver/stops/:id/proofs', requireRole('driver'), upload.array('photos', 10), async (req, res) => {
  try {
    const { data: stop, error: stopError } = await admin
      .from('tour_stops')
      .select('id, tour_id')
      .eq('id', req.params.id)
      .single();
    if (stopError) throw stopError;

    const files = req.files || [];
    for (const file of files) {
      const filePath = `${stop.tour_id}/${stop.id}/${Date.now()}-${sanitizeFileName(file.originalname)}`;

      const { error: uploadError } = await admin.storage.from(PROOF_BUCKET).upload(filePath, file.buffer, {
        contentType: file.mimetype,
        upsert: false
      });
      if (uploadError) throw uploadError;

      const { error: proofError } = await admin.from('proof_photos').insert({
        tour_id: stop.tour_id,
        stop_id: stop.id,
        driver_profile_id: req.session.user.profile_id,
        file_path: filePath,
        note: String(req.body.note || '').trim() || null
      });
      if (proofError) throw proofError;
    }

    flash(req, 'Preuve(s) envoyée(s).');
    res.redirect(`/driver/tours/${stop.tour_id}`);
  } catch (error) {
    console.error(error);
    flash(req, `Erreur upload preuve : ${error.message}`);
    res.redirect('/driver');
  }
});

app.post('/driver/tours/:id/location', requireRole('driver'), async (req, res) => {
  try {
    const latitude = Number(req.body.latitude);
    const longitude = Number(req.body.longitude);
    const accuracy = req.body.accuracy ? Number(req.body.accuracy) : null;

    const { data: previousPoint, error: previousError } = await admin
      .from('gps_tracking_points')
      .select('*')
      .eq('tour_id', req.params.id)
      .order('tracked_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (previousError) throw previousError;

    const { error: insertError } = await admin.from('gps_tracking_points').insert({
      tour_id: req.params.id,
      driver_profile_id: req.session.user.profile_id,
      latitude,
      longitude,
      accuracy_meters: accuracy,
      tracked_at: nowIso()
    });
    if (insertError) throw insertError;

    let kmToAdd = 0;
    if (previousPoint) {
      kmToAdd = haversineKm(
        Number(previousPoint.latitude),
        Number(previousPoint.longitude),
        latitude,
        longitude
      );
    }

    const { data: tour, error: tourError } = await admin
      .from('tours')
      .select('total_km')
      .eq('id', req.params.id)
      .single();
    if (tourError) throw tourError;

    const newKm = Number(tour.total_km || 0) + kmToAdd;

    const { error: updateError } = await admin
      .from('tours')
      .update({ total_km: Number(newKm.toFixed(2)) })
      .eq('id', req.params.id);
    if (updateError) throw updateError;

    res.json({ ok: true, km: Number(newKm.toFixed(2)) });
  } catch (error) {
    console.error(error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/client', requireRole('client'), async (req, res) => {
  try {
    const tours = await fetchTours({ client_profile_id: req.session.user.profile_id });
    res.render('client/dashboard', { title: 'Suivi client', tours });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur suivi client');
  }
});

app.get('/client/tours/:id', requireRole('client'), async (req, res) => {
  try {
    const tour = await fetchTourById(req.params.id);
    if (!tour || tour.client_id !== req.session.user.profile_id) {
      return res.status(404).send('Tournée introuvable');
    }

    res.render('client/tour', { title: tour.reference, tour });
  } catch (error) {
    console.error(error);
    res.status(500).send('Erreur détail client');
  }
});

app.get('/api/tours/:id/status', requireAuth, async (req, res) => {
  try {
    const tour = await fetchTourById(req.params.id);
    if (!tour) return res.status(404).json({ ok: false, error: 'Tour introuvable' });

    const isAllowed =
      req.session.user.role === 'dispatch' ||
      (req.session.user.role === 'driver' && tour.driver_id === req.session.user.profile_id) ||
      (req.session.user.role === 'client' && tour.client_id === req.session.user.profile_id);

    if (!isAllowed) return res.status(403).json({ ok: false, error: 'Accès refusé' });

    return res.json({ ok: true, tour });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/media/proofs/:id', requireAuth, async (req, res) => {
  try {
    const { data: proof, error } = await admin
      .from('proof_photos')
      .select('*')
      .eq('id', req.params.id)
      .single();
    if (error) throw error;

    const tour = await fetchTourById(proof.tour_id);

    const isAllowed =
      req.session.user.role === 'dispatch' ||
      (req.session.user.role === 'driver' && tour.driver_id === req.session.user.profile_id) ||
      (req.session.user.role === 'client' && tour.client_id === req.session.user.profile_id);

    if (!isAllowed) return res.status(403).send('Accès refusé');

    const { data: fileData, error: fileError } = await admin.storage
      .from(PROOF_BUCKET)
      .download(proof.file_path);
    if (fileError) throw fileError;

    const arrayBuffer = await fileData.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    res.setHeader('Content-Type', fileData.type || 'application/octet-stream');
    res.send(buffer);
  } catch (error) {
    console.error(error);
    res.status(404).send('Preuve introuvable');
  }
});

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`InterGlobe Web MVP lancé sur http://localhost:${PORT}`);
  });
}

module.exports = app;