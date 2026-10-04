// Dry-run by default. Run --send only after verifying both production deployments.
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
require('dotenv').config({ quiet: true });
const { Client } = require('pg');
const nodemailer = require('nodemailer');
const release = 'ot-process-2026-10-04-v1';
const send = process.argv.includes('--send');
const base = path.join(__dirname, '..', 'docs', 'communications');
const html = fs.readFileSync(path.join(base, 'ot-process-2026-10-04.html'), 'utf8');
const text = fs.readFileSync(path.join(base, 'ot-process-2026-10-04.txt'), 'utf8');
const ledgerPath = process.env.OT_ANNOUNCEMENT_LEDGER || path.join('/var/lib/justice', `${release}.json`);
const hash = value => createHash('sha256').update(value).digest('hex');
const bool = (value, fallback = false) => value == null || value === '' ? fallback : /^(true|1|yes|si)$/i.test(value);

async function main() {
  const client = new Client({ host: process.env.DB_HOST, port: Number(process.env.DB_PORT || 5432), user: process.env.DB_USER, password: process.env.DB_PASS, database: process.env.DB_NAME, ssl: bool(process.env.DB_SSL) ? { rejectUnauthorized: false } : false });
  let rows;
  try {
    await client.connect();
    ({ rows } = await client.query('SELECT email FROM kpi_security.tb_user WHERE COALESCE(is_deleted, false) = false'));
  } finally { await client.end(); }
  const valid = rows.map(row => String(row.email || '').trim().toLowerCase()).filter(email => /^[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+$/.test(email));
  const emails = [...new Set(valid)].sort();
  const summary = { release, users: rows.length, users_without_valid_email: rows.length - valid.length, duplicate_addresses: valid.length - emails.length, recipients: emails.length, send };
  if (!send) { console.log(JSON.stringify(summary)); return; }
  const host = process.env.ALERT_SMTP_HOST || process.env.SMTP_HOST || process.env.MAIL_HOST;
  const port = Number(process.env.ALERT_SMTP_PORT || process.env.SMTP_PORT || process.env.MAIL_PORT || 587);
  const user = process.env.ALERT_SMTP_USER || process.env.SMTP_USER || process.env.MAIL_USER;
  const pass = process.env.ALERT_SMTP_PASS || process.env.SMTP_PASS || process.env.MAIL_PASS;
  const from = process.env.ALERT_EMAIL_FROM || process.env.MAIL_FROM_ADDRESS || process.env.SMTP_FROM_EMAIL;
  if (!host || !from) throw new Error('No hay configuración SMTP completa.');
  if (!emails.length) throw new Error('No se encontraron correos válidos.');
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  const lockPath = `${ledgerPath}.lock`;
  const lock = fs.openSync(lockPath, 'wx', 0o600);
  const transporter = nodemailer.createTransport({ host, port, secure: bool(process.env.ALERT_SMTP_SECURE || process.env.SMTP_SECURE || process.env.MAIL_SECURE, port === 465), auth: user ? { user, pass } : undefined, connectionTimeout: 20000, socketTimeout: 40000 });
  try {
    await transporter.verify();
    const ledger = fs.existsSync(ledgerPath) ? JSON.parse(fs.readFileSync(ledgerPath, 'utf8')) : { release, content_sha256: hash(html), messages: {} };
    if (ledger.release !== release || ledger.content_sha256 !== hash(html)) throw new Error('El registro de envío pertenece a otra versión del comunicado.');
    let sent = 0, skipped = 0, failed = 0;
    for (const email of emails) {
      const key = hash(email);
      if (ledger.messages[key]?.accepted_at) { skipped++; continue; }
      try {
        const info = await transporter.sendMail({ from: { name: 'Justice Company · Gestión de OT', address: from }, to: email, subject: 'Nuevo proceso de órdenes de trabajo acordado con Gerencia', html, text, messageId: `<${release}.${key.slice(0,24)}@${from.split('@')[1]}>` });
        if (!info.accepted?.length) throw new Error('SMTP no aceptó el destinatario');
        ledger.messages[key] = { accepted_at: new Date().toISOString(), message_id: info.messageId };
        sent++;
      } catch (error) {
        ledger.messages[key] = { failed_at: new Date().toISOString(), code: error.code || 'SEND_FAILED' };
        failed++;
      }
      fs.writeFileSync(`${ledgerPath}.tmp`, JSON.stringify(ledger, null, 2), { mode: 0o600 });
      fs.renameSync(`${ledgerPath}.tmp`, ledgerPath);
    }
    console.log(JSON.stringify({ ...summary, sent, previously_sent: skipped, failed, smtp_accepted_total: Object.values(ledger.messages).filter(row => row.accepted_at).length }));
    if (failed) process.exitCode = 1;
  } finally { transporter.close(); fs.closeSync(lock); fs.unlinkSync(lockPath); }
}
main().catch(error => { console.error(error.code || error.message); process.exitCode = 1; });
