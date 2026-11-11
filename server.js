const express = require('express');
const { ExpressPeerServer } = require('peer');

const app = express();
const server = app.listen(process.env.PORT || 10000);

app.use('/peerjs', ExpressPeerServer(server, {
  path: '/',
  proxied: true,
  allow_discovery: false
}));

app.use(express.static(__dirname));
