using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

namespace RabbitMQHelper2
{
    internal class Program
    {
        private static ConnectionFactory factory;
        private static object lockObj = new object();
        static void Main(string[] args)
        {
            Console.WriteLine("Hello, World!");

            string input = "";
            while (input!="0") 
            {
                Console.WriteLine("请输入您的命令！");
                input = Console.ReadLine();
                switch (input) 
                {
                    case "1":
                        Console.WriteLine("开始生产");
                        SimpleSendMsg();
                        break;
                    case "2":
                        Console.WriteLine("开始消费");
                        SimpleConsumer();
                        break;
                    case "3":
                        Console.WriteLine("工作队列也称为公平性队列模式");
                        WorkerSendMsg();
                        break;
                    case "4":
                        Console.WriteLine("工作队列消费");
                        WorkerConsumer();
                        break;
                    case "5":
                        SendMessageFanout();
                        break;
                    case "6":
                        FanoutConsumer();
                        break;
                    case "7":
                        SendMessageDirect();
                        break;
                    case "8":
                        DirectConsumer();
                        break;
                    case "9":
                        SendMessageTopic();
                        break;
                    case "10":
                        TopicConsumer();
                        break;
                    case "11":
                        Console.WriteLine("Hello World!");
                        //启动服务端，正常逻辑是在另一个程序
                        RPCServer.RpcHandle();
                        //实例化客户端
                        var rpcClient = new RPCClient();
                        string message = $"消息id:{new Random().Next(1, 1000)}";
                        Console.WriteLine($"【客服端】RPC请求中，{message}");
                        //向服务端发送消息，等待回复
                        var response = rpcClient.Call(message);
                        Console.WriteLine("【客服端】收到回复响应:{0}", response);
                        rpcClient.Close();
                        break;
                       
                    default: Console.WriteLine("没有该指令"); break;
                }
              
               
            }
            Console.ReadKey();
        }

        /// <summary>
        /// 获取单个RabbitMQ连接
        /// </summary>
        /// <returns></returns>
        static IConnection GetConnection()
        {
            if (factory == null)
            {
                lock (lockObj)
                {
                    if (factory == null)
                    {
                        factory = new ConnectionFactory
                        {
                            HostName = "127.0.0.1",//ip
                            Port = 5672,//端口
                            UserName = "LuoCore",//账号
                            Password = "LuoCore",//密码
                            VirtualHost = "LuoCore" //虚拟主机
                        };
                    }
                }
            }
            return factory.CreateConnection();
        }

        public static void SimpleSendMsg()
        {
            string queueName = "simple_order";//队列名
                                              //创建连接
            using (var connection = GetConnection())
            {
                //创建信道
                using (var channel = connection.CreateModel())
                {
                    //创建队列:
                    //durable->是否持久化，
                    //exclusive->排他队列，只有创建它的连接(connection)能连，创建它的连接关闭，会自动删除队列。
                    //autoDelete：被消费后，消费者数量都断开时自动删除队列。
                    //arguments：创建队列的参数。
                    //发送消息参数解析：
                    //exchange：交换机，为什么能传空呢，因为RabbitMQ内置有一个默认的交换机，如果传空时，就会用默认交换机。
                    //routingKey：路由名称，这里用队列名称做路由key。
                    //mandatory：true告诉服务器至少将消息route到一个队列种，否则就将消息return给发送者；false：没有找到路由则消息丢弃。
                    channel.QueueDeclare(queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    for (var i = 0; i < 10; i++)
                    {
                        string message = $"Hello RabbitMQ MessageHello,{i + 1}";
                        var body = Encoding.UTF8.GetBytes(message);//发送消息
                        channel.BasicPublish(exchange: "", routingKey: queueName, mandatory: false, basicProperties: null, body);
                        Console.WriteLine($"发送消息到队列:{queueName},内容:{message}");
                    }
                }
            }
        }



        public static void SimpleConsumer()
        {
            
            string queueName = "simple_order";
            var connection = GetConnection();
            {
                //创建信道
                var channel = connection.CreateModel();
                {
                    //创建队列
                    channel.QueueDeclare(queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    var consumer = new EventingBasicConsumer(channel);
                    int i = 0;
                    consumer.Received += (model, ea) =>
                    {
                        //消费者业务处理
                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine($"{i},队列{queueName}消费消息长度:{message.Length}，内容：" + message);
                        i++;
                    };
                    channel.BasicConsume(queueName, true, consumer);
                }

            }

        }


        /// <summary>
        /// 工作队列模式
        /// </summary>
        public static void WorkerSendMsg()
        {
            string queueName = "worker_order";//队列名
                                              //创建连接
            using (var connection = GetConnection())
            {
                //创建信道
                using (var channel = connection.CreateModel())
                {
                    //参数durable：true，需要持久化，实际项目中肯定需要持久化的，不然重启RabbitMQ数据就会丢失了。
                    //创建队列
                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    for (var i = 0; i < 10; i++)
                    {
                        string message = $"Hello RabbitMQ MessageHello,{i + 1}";
                        var body = Encoding.UTF8.GetBytes(message);
                        //发送消息到rabbitmq
                        channel.BasicPublish(exchange: "", routingKey: queueName, mandatory: false, basicProperties: null, body);
                        Console.WriteLine($"工作队列模式【发送消息到队列:{queueName},内容:{message}");
                    }
                }
            }
        }


        public static void WorkerConsumer()
        {
            string queueName = "worker_order";
            var connection = GetConnection();
            {
                //创建信道
                var channel = connection.CreateModel();
                {
                    //创建队列
                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    var consumer = new EventingBasicConsumer(channel);
                    //prefetchCount:1来告知RabbitMQ,不要同时给一个消费者推送多于 N 个消息，也确保了消费速度和性能
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                    int i = 1;
                    int index = new Random().Next(10);
                    consumer.Received += (model, ea) =>
                    {
                        //处理业务
                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine($"{i},消费者:{index},队列{queueName}消费消息长度:{message.Length}");
                        channel.BasicAck(ea.DeliveryTag, false); //消息ack确认，告诉mq这条队列处理完，可以从mq删除了               Thread.Sleep(1000);                        i++;
                    };
                    channel.BasicConsume(queueName, autoAck: false, consumer);
                }
            }
        }



        /// <summary>
        /// 发布订阅， 扇形队列
        /// </summary>
        public static void SendMessageFanout()
        {
            //创建连接
            using (var connection = GetConnection())
            {
                //创建信道
                using (var channel = connection.CreateModel())
                {
                    string exchangeName = "fanout_exchange";
                    //创建交换机,fanout类型
                    channel.ExchangeDeclare(exchangeName, ExchangeType.Fanout);
                    string queueName1 = "fanout_queue1";
                    string queueName2 = "fanout_queue2";
                    string queueName3 = "fanout_queue3";
                    //创建队列
                    channel.QueueDeclare(queueName1, false, false, false);
                    channel.QueueDeclare(queueName2, false, false, false);
                    channel.QueueDeclare(queueName3, false, false, false);

                    //把创建的队列绑定交换机,routingKey不用给值，给了也没意义的
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: "");
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: "");
                    channel.QueueBind(queue: queueName3, exchange: exchangeName, routingKey: "");
                    //向交换机写10条消息
                    for (int i = 0; i < 10; i++)
                    {
                        string message = $"RabbitMQ Fanout {i + 1} Message";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "", null, body);
                        Console.WriteLine($"发送Fanout消息:{message}");
                    }
                }
            }
        }
        public static void FanoutConsumer()
        {
            string queueName1 = "fanout_queue1";
          
            var connection = GetConnection();
            {
                //创建信道
                var channel = connection.CreateModel();
                {
                    //创建队列
                    channel.QueueDeclare(queueName1, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    var consumer = new EventingBasicConsumer(channel);
                    int i = 1;
                    int index = new Random().Next(10);
                    consumer.Received += (model, ea) =>
                    {
                        //处理业务
                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine($"{i},消费者:{index},队列{queueName1}消费消息长度:{message.Length}");
                        channel.BasicAck(ea.DeliveryTag, false); //消息ack确认，告诉mq这条队列处理完，可以从mq删除了               Thread.Sleep(1000);                        i++;
                    };
                    channel.BasicConsume(queueName1, autoAck: false, consumer);



                    
                }
            }
        }



        /// <summary>
        /// 路由模式，点到点直连队列
        /// </summary>
        public static void SendMessageDirect()
        {
            //创建连接
            using (var connection = GetConnection())
            {
                //创建信道
                using (var channel = connection.CreateModel())
                {
                    //声明交换机对象,fanout类型
                    string exchangeName = "direct_exchange";
                    channel.ExchangeDeclare(exchangeName, ExchangeType.Direct);
                    //创建队列
                    string queueName1 = "direct_errorlog";
                    string queueName2 = "direct_alllog";
                    channel.QueueDeclare(queueName1, true, false, false);
                    channel.QueueDeclare(queueName2, true, false, false);

                    //把创建的队列绑定交换机,direct_errorlog队列只绑定routingKey:error
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: "error");
                    //direct_alllog队列绑定routingKey:error,info
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: "info");
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: "error");
                    //向交换机写10条错误日志和10条Info日志
                    for (int i = 0; i < 10; i++)
                    {
                        string message = $"RabbitMQ Direct {i + 1} error Message";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "error", null, body);
                        Console.WriteLine($"发送Direct消息error:{message}");

                        string message2 = $"RabbitMQ Direct {i + 1} info Message";
                        var body2 = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "info", null, body2);
                        Console.WriteLine($"info:{message2}");

                    }
                }
            }
        }



        public static void DirectConsumer()
        {
            string queueName = "direct_errorlog";
            var connection = GetConnection();
            {
                //创建信道
                var channel = connection.CreateModel();
                {
                    //创建队列
                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    var consumer = new EventingBasicConsumer(channel);
                    ///prefetchCount:1来告知RabbitMQ,不要同时给一个消费者推送多于 N 个消息，也确保了消费速度和性能
                    ///global：是否设为全局的
                    ///prefetchSize:单条消息大小，通常设0，表示不做限制
                    //是autoAck=false才会有效
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: true);
                    int i = 1;
                    consumer.Received += (model, ea) =>
                    {
                        //处理业务
                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine($"{i},队列{queueName}消费消息长度:{message.Length}");
                        channel.BasicAck(ea.DeliveryTag, false); //消息ack确认，告诉mq这条队列处理完，可以从mq删除了
                        i++;
                    };
                    channel.BasicConsume(queueName, autoAck: false, consumer);
                }
            }
        }



        public static void SendMessageTopic()
        {
            //创建连接
            using (var connection = GetConnection())
            {
                //创建信道
                using (var channel = connection.CreateModel())
                {
                    //声明交换机对象,fanout类型
                    string exchangeName = "topic_exchange";
                    channel.ExchangeDeclare(exchangeName, ExchangeType.Topic);
                    //队列名
                    string queueName1 = "topic_queue1";
                    string queueName2 = "topic_queue2";
                    //路由名
                    string routingKey1 = "*.orange.*";
                    string routingKey2 = "*.*.rabbit";
                    string routingKey3 = "lazy.#";
                    channel.QueueDeclare(queueName1, true, false, false);
                    channel.QueueDeclare(queueName2, true, false, false);

                    //把创建的队列绑定交换机,routingKey指定routingKey
                    channel.QueueBind(queue: queueName1, exchange: exchangeName, routingKey: routingKey1);
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: routingKey2);
                    channel.QueueBind(queue: queueName2, exchange: exchangeName, routingKey: routingKey3);
                    //向交换机写10条消息
                    for (int i = 0; i < 10; i++)
                    {
                        string message = $"RabbitMQ Direct {i + 1} Message";
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchangeName, routingKey: "aaa.orange.rabbit", null, body);
                        channel.BasicPublish(exchangeName, routingKey: "lazy.aa.rabbit", null, body);
                        Console.WriteLine($"发送Topic消息:{message}");
                    }
                }
            }
        }


        public static void TopicConsumer()
        {
            string queueName = "topic_queue2";
            var connection = GetConnection();
            {
                //创建信道
                var channel = connection.CreateModel();
                {
                    //创建队列
                    channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    var consumer = new EventingBasicConsumer(channel);
                    ///prefetchCount:1来告知RabbitMQ,不要同时给一个消费者推送多于 N 个消息，也确保了消费速度和性能
                    ///global：是否设为全局的
                    ///prefetchSize:单条消息大小，通常设0，表示不做限制
                    //是autoAck=false才会有效
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: true);
                    int i = 1;
                    consumer.Received += (model, ea) =>
                    {
                        //处理业务
                        var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Console.WriteLine($"{i},主题模式队列{queueName}消费消息长度:{message.Length}");
                        channel.BasicAck(ea.DeliveryTag, false); //消息ack确认，告诉mq这条队列处理完，可以从mq删除了
                        i++;
                    };
                    channel.BasicConsume(queueName, autoAck: false, consumer);
                }
            }
        }



        public class RPCServer
        {
            public static void RpcHandle()
            {

                var connection = GetConnection();
                {
                    var channel = connection.CreateModel();
                    {
                        string queueName = "rpc_queue";
                        channel.QueueDeclare(queue: queueName, durable: false,
                          exclusive: false, autoDelete: false, arguments: null);
                        channel.BasicQos(0, 1, false);
                        var consumer = new EventingBasicConsumer(channel);
                        channel.BasicConsume(queue: queueName,
                          autoAck: false, consumer: consumer);
                        Console.WriteLine("【服务端】等待RPC请求...");

                        consumer.Received += (model, ea) =>
                        {
                            string response = null;

                            var body = ea.Body.ToArray();
                            var props = ea.BasicProperties;
                            var replyProps = channel.CreateBasicProperties();
                            replyProps.CorrelationId = props.CorrelationId;

                            try
                            {
                                var message = Encoding.UTF8.GetString(body);
                                Console.WriteLine($"【服务端】接收到数据:{message},开始处理");
                                response = $"消息:{message},处理完成";
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("错误:" + e.Message);
                                response = "";
                            }
                            finally
                            {
                                var responseBytes = Encoding.UTF8.GetBytes(response);
                                channel.BasicPublish(exchange: "", routingKey: props.ReplyTo,
                                  basicProperties: replyProps, body: responseBytes);
                                channel.BasicAck(deliveryTag: ea.DeliveryTag,
                                  multiple: false);
                            }
                        };
                    }
                }
            }
        }

        public class RPCClient
        {
            private readonly IConnection connection;
            private readonly IModel channel;
            private readonly string replyQueueName;
            private readonly EventingBasicConsumer consumer;
            private readonly BlockingCollection<string> respQueue = new BlockingCollection<string>();
            private readonly IBasicProperties props;

            public RPCClient()
            {
                connection = GetConnection();

                channel = connection.CreateModel();
                replyQueueName = channel.QueueDeclare().QueueName;
                consumer = new EventingBasicConsumer(channel);

                props = channel.CreateBasicProperties();
                var correlationId = Guid.NewGuid().ToString();
                props.CorrelationId = correlationId; //给消息id
                props.ReplyTo = replyQueueName;//回调的队列名，Client关闭后会自动删除

                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var response = Encoding.UTF8.GetString(body);
                    //监听的消息Id和定义的消息Id相同代表这条消息服务端处理完成
                    if (ea.BasicProperties.CorrelationId == correlationId)
                    {
                        respQueue.Add(response);
                    }
                };

                channel.BasicConsume(
                    consumer: consumer,
                    queue: replyQueueName,
                    autoAck: true);
            }

            public string Call(string message)
            {
                var messageBytes = Encoding.UTF8.GetBytes(message);
                //发送消息
                channel.BasicPublish(
                    exchange: "",
                    routingKey: "rpc_queue",
                    basicProperties: props,
                    body: messageBytes);
                //等待回复
                return respQueue.Take();
            }

            public void Close()
            {
                connection.Close();
            }
        }
    }
}