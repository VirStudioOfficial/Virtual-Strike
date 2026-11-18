/* Virtual Strike — سرور لن (بدون نیاز به نصب پکیج)
   اجرا:  node server.js [port]
   کارش: ۱) فایل‌های بازی (index.html + مدل‌ها) رو سرو می‌کنه
         ۲) یه رله‌ی WebSocket می‌ده که جای Trystero/WebRTC رو می‌گیره */
const http = require('http'), fs = require('fs'), path = require('path'), os = require('os'), crypto = require('crypto');
const PORT = +(process.argv[2] || process.env.PORT || 8080);
const ROOT = __dirname;
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json',
  '.glb': 'model/gltf-binary', '.gltf': 'model/gltf+json', '.fbx': 'application/octet-stream', '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg',
  '.wav': 'audio/wav', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp', '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon', '.woff2': 'font/woff2', '.woff': 'font/woff', '.ttf': 'font/ttf', '.bin': 'application/octet-stream' };

/* ---------- HTTP: فایل استاتیک ---------- */
const server = http.createServer((req, res) => {
  let p;
  try { p = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch (e) { res.writeHead(400); return res.end(); }
  if (p === '/') p = '/index.html';
  const fp = path.join(ROOT, path.normalize(p));
  if (!fp.startsWith(ROOT) || /(^|[\\/])server\.js$/.test(fp)) { res.writeHead(403); return res.end('forbidden'); }
  fs.stat(fp, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404); return res.end('not found: ' + p); }
    const ext = path.extname(fp).toLowerCase();
    const h = { 'Content-Type': MIME[ext] || 'application/octet-stream', 'Cache-Control': 'no-cache' };
    if (ext === '.html') {   // به صفحه می‌گیم «لوکال» هستی → از WebSocket استفاده کن
      let s = fs.readFileSync(fp, 'utf8');
      s = s.replace('<head>', '<head><script>window.VS_LOCAL=1</script>');
      h['Content-Length'] = Buffer.byteLength(s); res.writeHead(200, h); return res.end(s);
    }
    h['Content-Length'] = st.size; h['Accept-Ranges'] = 'bytes';
    const m = /bytes=(\d*)-(\d*)/.exec(req.headers.range || '');
    if (m && (m[1] || m[2])) {
      let a = m[1] ? +m[1] : st.size - +m[2], b = m[1] && m[2] ? +m[2] : st.size - 1;
      if (a > b || a >= st.size) { res.writeHead(416, { 'Content-Range': 'bytes */' + st.size }); return res.end(); }
      b = Math.min(b, st.size - 1);
      h['Content-Range'] = `bytes ${a}-${b}/${st.size}`; h['Content-Length'] = b - a + 1; res.writeHead(206, h);
      return fs.createReadStream(fp, { start: a, end: b }).pipe(res);
    }
    res.writeHead(200, h); fs.createReadStream(fp).pipe(res);
  });
});

/* ---------- WebSocket (دستی، بدون پکیج) ---------- */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function wsSend(sock, str, op = 1) {
  if (sock.destroyed) return;
  const b = Buffer.from(str), n = b.length; let h;
  if (n < 126) h = Buffer.from([0x80 | op, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  sock.write(Buffer.concat([h, b]));
}
const MAXMSG = 1 << 20;
function wsParse(sock, onMsg, onClose) {
  let buf = Buffer.alloc(0), frag = null;
  sock.on('data', d => {
    buf = Buffer.concat([buf, d]);
    for (;;) {
      if (buf.length < 2) return;
      const fin = buf[0] & 0x80, op = buf[0] & 15, masked = buf[1] & 0x80; let len = buf[1] & 127, o = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); o = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); o = 10; }
      if (len > MAXMSG) { sock.destroy(); return; }
      if (buf.length < o + (masked ? 4 : 0) + len) return;
      let payload;
      if (masked) { const k = buf.slice(o, o + 4); payload = Buffer.from(buf.slice(o + 4, o + 4 + len)); for (let i = 0; i < len; i++) payload[i] ^= k[i & 3]; o += 4 + len; }
      else { payload = buf.slice(o, o + len); o += len; }
      buf = buf.slice(o);
      if (op === 8) { try { sock.end(); } catch (e) {} return; }
      if (op === 9) { wsSend(sock, payload.toString('binary'), 10); continue; }   // ping → pong
      if (op === 10) continue;
      if (op === 1 || op === 2 || op === 0) {
        if (op !== 0) frag = []; if (frag) frag.push(payload);
        if (fin && frag) { const full = Buffer.concat(frag).toString('utf8'); frag = null; onMsg(full); }
      }
    }
  });
  sock.on('end', () => sock.destroy());   // http.Server = allowHalfOpen → بدون این، تب بسته‌شده هیچ‌وقت از لیست پاک نمی‌شد
  sock.on('close', onClose); sock.on('error', () => sock.destroy());
}

/* ---------- رله: id → سوکت، cid → دو سر اتصال ---------- */
const peers = new Map();   // id → client
const conns = new Map();   // cid → {a, b}  (a=کسی که connect زده، b=صاحب id)
let CID = 0;
const tx = (c, o) => wsSend(c.sock, JSON.stringify(o));
function killConn(cid, from) {
  const k = conns.get(cid); if (!k) return; conns.delete(cid);
  const other = k.a === from ? k.b : k.a; if (other) { other.cids.delete(cid); tx(other, { k: 'x', cid }); }
  if (from) from.cids.delete(cid);
}
server.on('upgrade', (req, sock) => {
  if (new URL(req.url, 'http://x').pathname !== '/ws' || !req.headers['sec-websocket-key']) { sock.destroy(); return; }
  const acc = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true); sock.setKeepAlive(true, 15000);   // NoDelay = بدون تأخیر نیگل → پینگ کمتر
  const c = { sock, ids: new Set(), cids: new Set() };
  const addr = req.socket.remoteAddress;
  wsParse(sock, str => {
    let d; try { d = JSON.parse(str); } catch (e) { return; }
    if (d.k === 'reg') {
      const id = String(d.id || '');
      if (!id || peers.has(id)) return tx(c, { k: 'reg', id, ok: 0 });
      peers.set(id, c); c.ids.add(id); tx(c, { k: 'reg', id, ok: 1 });
    } else if (d.k === 'unreg') {
      if (peers.get(d.id) === c) { peers.delete(d.id); c.ids.delete(d.id); }
    } else if (d.k === 'conn') {
      const o = peers.get(String(d.to));
      if (!o) return tx(c, { k: 'na', t: d.t });
      const cid = ++CID; conns.set(cid, { a: c, b: o }); c.cids.add(cid); o.cids.add(cid);
      tx(o, { k: 'in', cid, to: d.to }); tx(c, { k: 'out', t: d.t, cid });
    } else if (d.k === 'd') {
      const k = conns.get(d.cid); if (!k) return;
      const other = k.a === c ? k.b : k.a; if (other) tx(other, { k: 'd', cid: d.cid, m: d.m });
    } else if (d.k === 'x') { killConn(d.cid, c); }
    else if (d.k === 'ping') tx(c, { k: 'pong', t: d.t });
  }, () => {
    for (const id of c.ids) if (peers.get(id) === c) peers.delete(id);
    for (const cid of [...c.cids]) killConn(cid, c);
    console.log('[-] قطع شد', addr, '| آنلاین:', peers.size);
  });
  console.log('[+] وصل شد', addr);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n=== Virtual Strike LAN ===');
  console.log('خودت:      http://localhost:' + PORT);
  console.log('دوستات (IP شبکه مجازی خودت رو بهشون بده):');
  for (const [name, list] of Object.entries(os.networkInterfaces()))
    for (const i of list) if (i.family === 'IPv4' && !i.internal) {
      const virt = /radmin|zerotier|hamachi|tailscale|vpn|virtual|tap|tun/i.test(name) || /^(26|25|5)\./.test(i.address) ? '   <-- احتمالاً شبکه مجازی' : '';
      console.log('   http://' + i.address + ':' + PORT + '   (' + name + ')' + virt);
    }
  console.log('\nاگه وصل نشد: فایروال ویندوز رو برای Node.js / پورت ' + PORT + ' باز کن.\n');
});
