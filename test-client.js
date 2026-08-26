const WebSocket = require('ws')


const ws = new WebSocket(
  'ws://localhost:3000'
)



ws.on('open',()=>{


  console.log(
    '测试客户端B连接成功'
  )



  // 修改这里的房间码

  ws.send(JSON.stringify({

    type:'joinRoom',

    roomCode:'847071'

  }))



})





ws.on('message',(data)=>{


  const message =
    JSON.parse(
      data.toString()
    )



  console.log(
    'B收到:',
    message
  )


})