const WebSocket = require('ws')


const ws = new WebSocket(
  'wss://together-server-sxyr.onrender.com'
)


ws.on('open',()=>{

  console.log(
    '测试房主连接成功'
  )


  ws.send(JSON.stringify({

    type:'createRoom'

  }))


})


ws.on('message',(data)=>{

  const message =
    JSON.parse(
      data.toString()
    )


  console.log(
    '房主收到:',
    message
  )

})