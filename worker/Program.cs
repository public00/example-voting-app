using System;
using System.Data.Common;
using System.Diagnostics; 
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Newtonsoft.Json;
using Npgsql;
using Serilog;
using Serilog.Formatting.Json;
using StackExchange.Redis;

// NEW OpenTelemetry Usings
using OpenTelemetry.Trace;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;


namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
           
           var endpointUrl = Environment.GetEnvironmentVariable("DT_ENDPOINT_URL");
            var apiToken = Environment.GetEnvironmentVariable("DT_API_TOKEN");
            using var tracerProvider = Sdk.CreateTracerProviderBuilder()
                .AddSource("Worker.ProcessVote") 
                .SetResourceBuilder(ResourceBuilder.CreateDefault()
                    .AddService("Worker-Service") 
                )
                // Add instrumentation for standard libraries
                .AddHttpClientInstrumentation() 
                
                // Add the OTLP Exporter
                .AddOtlpExporter(opt =>
                {
                    opt.Endpoint = endpoint; 
                    opt.Protocol = OtlpExportProtocol.HttpProtobuf;
                    opt.Headers = token;
                })
                .Build();
            // ----------------------------------------
            
            // -------------------------------
            // Serilog setup
            // -------------------------------
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console(new JsonFormatter())
                .CreateLogger();

            try
            {
                // -------------------------------
                // Worker infrastructure
                // -------------------------------
                var pgsql = OpenDbConnection("Server=db;Username=postgres;Password=postgres;");
                var redisConn = OpenRedisConnection("redis");
                var redis = redisConn.GetDatabase();

                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                // Ensure the anonymous type definition includes the 'traceparent' field
                var definition = new { vote = "", voter_id = "", traceparent = "" };

                // -------------------------------
                // Main worker loop
                // -------------------------------
                while (true)
                {
                    Thread.Sleep(100);

                    if (redisConn == null || !redisConn.IsConnected)
                    {
                        Console.WriteLine("Reconnecting Redis");
                        redisConn = OpenRedisConnection("redis");
                        redis = redisConn.GetDatabase();
                    }

                    string json = redis.ListLeftPopAsync("votes").Result;

                    if (json != null)
                    {
                        var vote = JsonConvert.DeserializeAnonymousType(json, definition);

                        // Use the ActivitySource defined above ("Worker.ProcessVote") to create the Activity
                        var activitySource = new ActivitySource("Worker.ProcessVote");

                        using (var activity = activitySource.StartActivity("ProcessVoteFromQueue", ActivityKind.Consumer))
                        {
                            // --- CRITICAL FIX: Direct W3C Header Injection ---
                            if (!string.IsNullOrEmpty(vote.traceparent))
                            {
                                try 
                                {
                                    // SetParentId(string) attempts to parse the W3C traceparent header 
                                    // and set the current Activity's TraceId and ParentSpanId.
                                    activity?.SetParentId(vote.traceparent);
                                }
                                catch (Exception ex)
                                {
                                     Log.Error(ex, "Failed to set parent trace ID from Redis payload: {Traceparent}", vote.traceparent);
                                }
                            }
                            // ---------------------------------------------------------
                            
                            // Log context - The TraceId here MUST now match the Python Trace ID.
                            Log.ForContext("traceId", activity?.TraceId.ToString() ?? "null")
                               .ForContext("spanId", activity?.SpanId.ToString() ?? "null")
                               .Information(
                                   "Processing vote for {VoteChoice} by {VoterId}",
                                   vote.vote,
                                   vote.voter_id
                               );

                            if (!pgsql.State.Equals(System.Data.ConnectionState.Open))
                            {
                                Console.WriteLine("Reconnecting DB");
                                pgsql = OpenDbConnection(
                                    "Server=db;Username=postgres;Password=postgres;"
                                );
                            }
                            else
                            {
                                UpdateVote(pgsql, vote.voter_id, vote.vote);
                            }
                        }
                    }
                    else
                    {
                        keepAliveCommand.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
        }

        // -------------------------------------------------
        // Helpers (Unchanged)
        // -------------------------------------------------

        private static NpgsqlConnection OpenDbConnection(string connectionString)
        {
            // ... (unchanged) ...
            NpgsqlConnection connection;

            while (true)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    break;
                }
                catch (Exception)
                {
                    Console.Error.WriteLine("Waiting for db");
                    Thread.Sleep(1000);
                }
            }

            Console.Error.WriteLine("Connected to db");

            var command = connection.CreateCommand();
            command.CommandText = @"CREATE TABLE IF NOT EXISTS votes (
                                        id VARCHAR(255) NOT NULL UNIQUE,
                                        vote VARCHAR(255) NOT NULL
                                    )";
            command.ExecuteNonQuery();

            return connection;
        }

        private static ConnectionMultiplexer OpenRedisConnection(string hostname)
        {
            // ... (unchanged) ...
            var ipAddress = GetIp(hostname);
            Console.WriteLine($"Found redis at {ipAddress}");

            while (true)
            {
                try
                {
                    Console.Error.WriteLine("Connecting to redis");
                    return ConnectionMultiplexer.Connect(ipAddress);
                }
                catch (RedisConnectionException)
                {
                    Console.Error.WriteLine("Waiting for redis");
                    Thread.Sleep(1000);
                }
            }
        }

        private static string GetIp(string hostname)
            => Dns.GetHostEntryAsync(hostname)
                .Result
                .AddressList
                .First(a => a.AddressFamily == AddressFamily.InterNetwork)
                .ToString();

        private static void UpdateVote(
            NpgsqlConnection connection,
            string voterId,
            string vote)
        {
            // ... (unchanged) ...
            var command = connection.CreateCommand();

            try
            {
                command.CommandText =
                    "INSERT INTO votes (id, vote) VALUES (@id, @vote)";
                command.Parameters.AddWithValue("@id", voterId);
                command.Parameters.AddWithValue("@vote", vote);
                command.ExecuteNonQuery();
            }
            catch (DbException)
            {
                command.CommandText =
                    "UPDATE votes SET vote = @vote WHERE id = @id";
                command.ExecuteNonQuery();
            }
            finally
            {
                command.Dispose();
            }
        }
    }
}