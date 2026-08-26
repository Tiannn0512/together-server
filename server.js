const WebSocket = require('ws')

const PORT = process.env.PORT || 3000

const wss = new WebSocket.Server({

  host:'0.0.0.0',

  port:PORT

})


console.log(
  `一起听服务器启动，端口:${PORT}`
)





// 房间结构
//
// rooms={
//
//   "123456":{
//
//      clients:[ws,ws],
//
//      host:ws,
//
//      state:{
//          musicInfo:null,
//          currentTime:0,
//          playing:false,
//          timestamp:0
//      }
//
//   }
//
// }


const rooms = {}







// 创建随机6位房间码

function createRoomCode(){


  return Math.floor(

    100000 +

    Math.random()*900000

  ).toString()


}









// 广播消息

function broadcast(

  room,

  message,

  sender

){


  room.clients.forEach(

    client=>{


      if(

        client !== sender &&

        client.readyState === WebSocket.OPEN

      ){


        client.send(

          JSON.stringify(message)

        )


      }


    }

  )


}









// 更新房间状态

function updateRoomState(

  room,

  message

){


  if(!room.state)

    return



  switch(message.type){



    case 'musicChange':



      if(

        message.data &&

        message.data.musicInfo

      ){


        room.state.musicInfo =

          message.data.musicInfo


        room.state.currentTime = 0


      }


      break






    case 'play':



      room.state.playing = true



      if(

        message.data &&

        message.data.currentTime !== undefined

      ){


        room.state.currentTime =

          message.data.currentTime


      }


      break






    case 'pause':



      room.state.playing = false



      if(

        message.data &&

        message.data.currentTime !== undefined

      ){


        room.state.currentTime =

          message.data.currentTime


      }


      break






    case 'progress':



      if(

        message.data &&

        message.data.currentTime !== undefined

      ){


        room.state.currentTime =

          message.data.currentTime



        room.state.timestamp =

          Date.now()


      }


      break



  }


}









wss.on(

  'connection',

  (ws)=>{


    console.log(

      '客户端连接'

    )



    ws.roomCode = null

    ws.isHost = false







    ws.on(

      'message',

      (data)=>{



        let message
        
        try {
        

          message =  JSON.parse(

            data.toString()

          )
        
        } catch(e){


        console.log(

          '消息解析失败:',
          e

        )

        return

        }







        switch(message.type){







          // 创建房间

          case 'createRoom':{



            let code



            do{


              code =

                createRoomCode()



            }while(

              rooms[code]

            )







            const room = {


              clients:[

                ws

              ],



              host:ws,



              state:{


                musicInfo:null,


                currentTime:0,


                playing:false,


                timestamp:Date.now()


              }


            }







            rooms[code] =

              room







            ws.roomCode =

              code



            ws.isHost =

              true







            ws.send(

              JSON.stringify({


                type:'roomCreated',


                roomCode:code


              })

            )





            break

          }













          // 加入房间

          case 'joinRoom':{



            const code =

              message.roomCode





            const room =

              rooms[code]







            if(room){





              room.clients.push(

                ws

              )




              ws.roomCode =

                code




              ws.isHost =

                false







              ws.send(

                JSON.stringify({


                  type:'roomJoined',


                  roomCode:code


                })

              )









              // 返回当前状态

              if(room.state){



                ws.send(

                  JSON.stringify({


                    type:'syncState',


                    timestamp:

                      Date.now(),


                    data:

                      room.state


                  })

                )


              }









              // 通知房主

              room.clients.forEach(

                client=>{


                  if(

                    client !== ws &&

                    client.readyState === WebSocket.OPEN

                  ){


                    client.send(

                      JSON.stringify({


                        type:'userJoined'


                      })

                    )


                  }


                }

              )





            }else{





              ws.send(

                JSON.stringify({


                  type:'roomError',


                  message:'房间不存在'


                })

              )



            }





            break

          }












          // 主动退出房间

          case 'leaveRoom':{





            const code =

              ws.roomCode





            if(!code)

              break





            const room =

              rooms[code]





            if(!room)

              break







            room.clients =

              room.clients.filter(

                client=>client!==ws

              )








            // 如果房主退出

            if(

              room.host === ws

            ){





              room.clients.forEach(

                client=>{


                  if(

                    client.readyState === WebSocket.OPEN

                  ){



                    client.send(

                      JSON.stringify({


                        type:'roomError',


                        message:'房主退出，一起听结束'


                      })

                    )


                  }


                }

              )







              delete rooms[code]







            }else{





              broadcast(

                room,

                {


                  type:'userLeft'


                },

                ws

              )








              if(

                room.clients.length===0

              ){


                delete rooms[code]


              }



            }







            ws.roomCode = null

            ws.isHost = false






            break

          }














          // 普通同步消息

          default:{



            const room =

              rooms[ws.roomCode]





            if(!room)

              break







            // 只有房主控制

            if(!ws.isHost)

              break







            updateRoomState(

              room,

              message

            )








            broadcast(

              room,

              message,

              ws

            )






            break

          }







        }



      }

    )












    // 客户端断开

    ws.on(

      'close',

      ()=>{





        const code =

          ws.roomCode





        if(!code)

          return





        const room =

          rooms[code]





        if(!room)

          return







        room.clients =

          room.clients.filter(

            client=>client!==ws

          )








        // 房主断开

        if(

          room.host === ws

        ){





          room.clients.forEach(

            client=>{


              if(

                client.readyState === WebSocket.OPEN

              ){


                client.send(

                  JSON.stringify({


                    type:'roomError',


                    message:'房主断开，一起听结束'


                  })

                )


              }


            }

          )






          delete rooms[code]





          return

        }









        // 普通成员断开

        broadcast(

          room,

          {


            type:'userLeft'


          },

          ws

        )








        if(

          room.clients.length===0

        ){


          delete rooms[code]


        }





      }

    )





  }

)