const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, '..', 'web')));

// deviceId -> { socketId, connectedAt, lastFrameAt, meta }
const devices = new Map();
const socketToDevice = new Map();
const dashboards = new Set();
let totalFramesRelayed = 0;

function deviceListPayload() {
    const list = [];
    for (const [id, info] of devices) {
        list.push({
            deviceId: id,
            online: true,
            connectedAt: info.connectedAt,
            lastFrameAt: info.lastFrameAt || null,
            model: (info.meta && (info.meta.model || info.meta.manufacturer)) || '',
            manufacturer: (info.meta && info.meta.manufacturer) || '',
            phone: (info.meta && info.meta.phone) || '',
            sdk: (info.meta && info.meta.sdk) || null
        });
    }
    return list;
}

function broadcastDeviceList() {
    const list = deviceListPayload();
    for (const dashId of dashboards) {
        const s = io.sockets.sockets.get(dashId);
        if (s) s.emit('device-list', { devices: list });
    }
}

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        uptime: process.uptime(),
        devices: deviceListPayload(),
        deviceCount: devices.size,
        dashboardClients: dashboards.size,
        framesRelayed: totalFramesRelayed
    });
});

app.get('/devices', (req, res) => {
    res.json({ devices: deviceListPayload() });
});

const server = http.createServer(app);

const io = new Server(server, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
    pingTimeout: 60000,
    pingInterval: 25000,
    maxHttpBufferSize: 1e6,
    transports: ['websocket', 'polling']
});

io.on('connection', (socket) => {
    console.log(`[+] Connected: ${socket.id}`);

    socket.on('register-android', (payload) => {
        let deviceId = 'default';
        let meta = null;
        if (typeof payload === 'string' && payload.trim()) {
            deviceId = payload.trim();
        } else if (payload && typeof payload === 'object') {
            if (payload.deviceId) deviceId = String(payload.deviceId).trim() || 'default';
            if (payload.meta) meta = payload.meta;
        }

        const existing = devices.get(deviceId);
        if (existing && existing.socketId !== socket.id) {
            const oldSocket = io.sockets.sockets.get(existing.socketId);
            if (oldSocket) oldSocket.disconnect(true);
            socketToDevice.delete(existing.socketId);
        }

        devices.set(deviceId, {
            socketId: socket.id,
            connectedAt: Date.now(),
            lastFrameAt: null,
            meta: meta || null
        });
        socketToDevice.set(socket.id, deviceId);
        socket.data.isAndroid = true;
        socket.data.deviceId = deviceId;

        console.log(`[Android] Online: ${deviceId} model=${meta && meta.model ? meta.model : '?'} (${socket.id})`);
        broadcastDeviceList();

        for (const dashId of dashboards) {
            const s = io.sockets.sockets.get(dashId);
            if (s && s.data.watchingDeviceId === deviceId) {
                s.emit('android-status', { connected: true, deviceId });
            }
        }
    });

    socket.on('register-dashboard', () => {
        socket.data.isDashboard = true;
        socket.data.isStreaming = false;
        socket.data.watchingDeviceId = null;
        dashboards.add(socket.id);
        console.log(`[Dashboard] Connected: ${socket.id} (total: ${dashboards.size})`);
        socket.emit('device-list', { devices: deviceListPayload() });
    });

    socket.on('select-device', (deviceId) => {
        if (!socket.data.isDashboard) return;
        const id = (deviceId || '').toString().trim();
        socket.data.watchingDeviceId = id || null;
        const online = id && devices.has(id);
        socket.emit('android-status', { connected: !!online, deviceId: id });
        console.log(`[Dashboard] ${socket.id} watching: ${id || '(none)'} online=${!!online}`);
    });

    socket.on('start-stream', () => {
        if (!socket.data.isDashboard) return;
        socket.data.isStreaming = true;
        console.log(`[Dashboard] STARTED: ${socket.id} → ${socket.data.watchingDeviceId}`);
    });

    socket.on('stop-stream', () => {
        if (!socket.data.isDashboard) return;
        socket.data.isStreaming = false;
        console.log(`[Dashboard] STOPPED: ${socket.id}`);
    });

    // Dashboard → Android remote control
    socket.on('control', (payload) => {
        if (!socket.data.isDashboard) return;
        const deviceId = socket.data.watchingDeviceId;
        if (!deviceId) return;
        const info = devices.get(deviceId);
        if (!info) return;
        const androidSocket = io.sockets.sockets.get(info.socketId);
        if (androidSocket && androidSocket.connected) {
            androidSocket.emit('control', payload);
        }
    });

    socket.on('frame', (base64Data) => {
        if (!socket.data.isAndroid) return;
        const deviceId = socket.data.deviceId;
        if (!deviceId) return;

        const info = devices.get(deviceId);
        if (info) info.lastFrameAt = Date.now();

        totalFramesRelayed++;

        for (const dashId of dashboards) {
            const dashSocket = io.sockets.sockets.get(dashId);
            if (!dashSocket || !dashSocket.connected) continue;
            if (!dashSocket.data.isStreaming) continue;
            if (dashSocket.data.watchingDeviceId !== deviceId) continue;
            dashSocket.emit('frame', base64Data);
        }
    });

    socket.on('rotation', (meta) => {
        if (!socket.data.isAndroid) return;
        const deviceId = socket.data.deviceId;
        for (const dashId of dashboards) {
            const s = io.sockets.sockets.get(dashId);
            if (s && s.data.watchingDeviceId === deviceId) {
                s.emit('rotation', meta);
            }
        }
    });

    socket.on('disconnect', () => {
        console.log(`[-] Disconnected: ${socket.id}`);
        if (socket.data.isAndroid) {
            const deviceId = socket.data.deviceId || socketToDevice.get(socket.id);
            if (deviceId) {
                const info = devices.get(deviceId);
                if (info && info.socketId === socket.id) {
                    devices.delete(deviceId);
                }
                socketToDevice.delete(socket.id);
                console.log(`[Android] Offline: ${deviceId}`);
                broadcastDeviceList();
                for (const dashId of dashboards) {
                    const s = io.sockets.sockets.get(dashId);
                    if (s && s.data.watchingDeviceId === deviceId) {
                        s.emit('android-status', { connected: false, deviceId });
                    }
                }
            }
        }
        if (socket.data.isDashboard) {
            dashboards.delete(socket.id);
            console.log(`[Dashboard] Left (remaining: ${dashboards.size})`);
        }
    });
});

setInterval(() => {
    console.log(
        `[Stats] Devices: ${devices.size} | Dashboards: ${dashboards.size} | Frames: ${totalFramesRelayed}`
    );
}, 30000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`========================================`);
    console.log(` Screen Monitor Server (Multi-User + Control)`);
    console.log(` Port: ${PORT}`);
    console.log(`========================================`);
});
