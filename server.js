const WebSocket = require('ws')


const PORT = process.env.PORT || 3000

// 心跳间隔，用于清理已死连接
const HEARTBEAT_INTERVAL = 30000


const wss = new WebSocket.Server({
  host: '0.0.0.0',
  port: PORT,
})


console.log(`一起听服务器启动，端口:${PORT}`)



/*

房间结构：

rooms={

  "123456":{

    clients:[ws,ws],

    host:ws,

    state:{

      musicInfo:null,

      currentTime:0,

      playing:false,

      timestamp:0

    }

  }

}

*/



const rooms = {}




// 创建随机6位房间码
function createRoomCode() {
  return Math.floor(100000 + Math.random() * 900000).toString()
}



// 发送消息
function sendMessage(ws, message) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message))
  }
}



// 广播消息
function broadcast(room, message, sender) {
  room.clients.forEach(client => {
    if (client !== sender && client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(message))
    }
  })
}



// 获取当前在线人数
function getOnlineCount(room) {
  return room.clients.length
}



// 将连接从其当前所在房间移除（房主离开则关闭整个房间）
// closeMessage 为房主离开时通知其他成员的文案
function removeFromRoom(ws, closeMessage) {
  const code = ws.roomCode

  ws.roomCode = null
  ws.isHost = false

  if (!code) return

  const room = rooms[code]
  if (!room) return

  room.clients = room.clients.filter(client => client !== ws)

  // 房主离开，关闭整个房间
  if (room.host === ws) {
    room.clients.forEach(client => {
      sendMessage(client, {
        type: 'roomClosed',
        message: closeMessage,
      })
    })

    delete rooms[code]
    return
  }

  // 普通成员离开
  broadcast(room, {
    type: 'userLeft',
    onlineCount: getOnlineCount(room),
  }, ws)

  if (room.clients.length === 0) {
    delete rooms[code]
  }
}



// 更新房间状态
function updateRoomState(room, message) {
  if (!room.state) return

  switch (message.type) {
    case 'musicChange':
      if (message.data && message.data.musicInfo) {
        room.state.musicInfo = message.data.musicInfo
        room.state.currentTime = 0
      }
      break

    case 'play':
      room.state.playing = true
      if (message.data && message.data.currentTime !== undefined) {
        room.state.currentTime = message.data.currentTime
      }
      break

    case 'pause':
      room.state.playing = false
      if (message.data && message.data.currentTime !== undefined) {
        room.state.currentTime = message.data.currentTime
      }
      break

    case 'progress':
      if (message.data && message.data.currentTime !== undefined) {
        room.state.currentTime = message.data.currentTime
        room.state.timestamp = Date.now()
      }
      break
  }
}




wss.on('connection', (ws) => {
  console.log('客户端连接')

  ws.roomCode = null
  ws.isHost = false
  ws.isAlive = true

  ws.on('pong', () => {
    ws.isAlive = true
  })



  ws.on('message', (data) => {

    let message

    try {
      message = JSON.parse(data.toString())
    } catch (e) {
      console.log('消息解析失败:', e)
      return
    }

    if (!message || typeof message.type !== 'string') return


    switch (message.type) {



      // 创建房间
      case 'createRoom': {
        // 先退出可能存在的旧房间，避免旧房间残留幽灵成员
        removeFromRoom(ws, '房主退出，一起听结束')

        let code

        do {
          code = createRoomCode()
        } while (rooms[code])


        const room = {
          clients: [ws],
          host: ws,
          state: {
            musicInfo: null,
            currentTime: 0,
            playing: false,
            timestamp: Date.now(),
          },
        }


        rooms[code] = room


        ws.roomCode = code
        ws.isHost = true


        sendMessage(ws, {
          type: 'roomCreated',
          roomCode: code,
          onlineCount: getOnlineCount(room),
        })

        break
      }



      // 加入房间
      case 'joinRoom': {
        const code = typeof message.roomCode === 'string' ? message.roomCode : null

        const room = code ? rooms[code] : undefined

        if (!room) {
          sendMessage(ws, {
            type: 'roomError',
            message: '房间不存在',
          })
          break
        }

        // 先退出可能存在的旧房间
        removeFromRoom(ws, '房主退出，一起听结束')

        room.clients.push(ws)


        ws.roomCode = code
        ws.isHost = false


        sendMessage(ws, {
          type: 'roomJoined',
          roomCode: code,
          onlineCount: getOnlineCount(room),
        })


        // 返回当前同步状态
        if (room.state) {
          sendMessage(ws, {
            type: 'syncState',
            timestamp: Date.now(),
            data: room.state,
          })
        }


        // 通知其他成员
        broadcast(room, {
          type: 'userJoined',
          onlineCount: getOnlineCount(room),
        }, ws)

        break
      }



      // 主动退出
      case 'leaveRoom': {
        removeFromRoom(ws, '房主退出，一起听结束')
        break
      }



      // 普通同步消息
      default: {
        const room = rooms[ws.roomCode]

        if (!room) break

        // 只有房主控制
        if (!ws.isHost) break


        updateRoomState(room, message)


        // 转发时统一重打服务器时间戳，
        // 听客据此做延迟补偿，避免双端时钟偏差导致进度跳变
        broadcast(room, {
          ...message,
          timestamp: Date.now(),
        }, ws)

        break
      }


    }

  })



  // 客户端断开
  ws.on('close', () => {
    removeFromRoom(ws, '房主断开，一起听结束')
  })

})



// 心跳：主动探测并终止已死连接（网络异常时 close 事件可能迟迟不触发，导致房间泄漏）
const heartbeatTimer = setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) {
      ws.terminate()
      return
    }

    ws.isAlive = false
    ws.ping(() => {})
  })
}, HEARTBEAT_INTERVAL)

wss.on('close', () => {
  clearInterval(heartbeatTimer)
})
