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

// OpenTelemetry
using OpenTelemetry;
using OpenTelemetry.Trace;
using OpenTelemetry.Resources;
using OpenTelemetry.Exporter;
using OpenTelemetry.Instrumentation.Http;

namespace Worker
{
    public class Program
    {
        // Global ActivitySource for manual spans
        private static readonly ActivitySource ActivitySource = new ActivitySource("Worker.ProcessVote");

        public static int Main(string[] args)
        {
            // --- Dynatrace OTLP settings ---
            var endpointUrl = Environment.GetEnvironmentVariable("DT_ENDPOINT_URL");
            var apiToken = Environment.GetEnvironmentVariable("DT_AUTH_TOKEN");

            // --- OpenTelemetry Tracer ---
            using var tracerProvider = Sdk.CreateTracerProviderBuilder()
                .AddSource("Worker.ProcessVote")
                .SetResourceBuilder(ResourceBuilder.CreateDefault()
                    .AddService("Worker-Service"))
                .AddHttpClientInstrumentation()
                .AddOtlpExporter(opt =>
                {
                    opt.Endpoint = new Uri(endpointUrl);
                    opt.Protocol = OtlpExportProtocol.HttpProtobuf;
                    opt.Headers = $"Api-Token={apiToken}";
                })
                .Build();

            // --- Serilog setup ---
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console(new JsonFormatter())
                .CreateLogger();

            try
            {
                var pgsql = OpenDbConnection("Server=db;Username=postgres;Password=postgres;");
                var redisConn = OpenRedisConnection("redis");
                var redis = redisConn.GetDatabase();

                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                var definition = new { vote = "", voter_id = "", traceparent = "" };

                while (true)
                {
                    Thread.Sleep(100);

                    if (redisConn == null || !redisConn.IsConnected)
                    {
                        Console.WriteLine("Reconnecting Redis");
                        redisConn = OpenRedisConnection("redis");
                        redis = redisConn.GetDatabase();
                    }

                    // Pop from Redis
                    string json = redis.ListLeftPopAsync("votes").Result;

                    if (json != null)
                    {
                        var vote = JsonConvert.DeserializeAnonymousType(json, definition);

                        // Manual span for processing a vote
                        using var activity = ActivitySource.StartActivity("ProcessVoteFromQueue", ActivityKind.Consumer);

                        // Manually trace Redis operation
                        using var redisActivity = ActivitySource.StartActivity("Redis.ListLeftPop");

                        if (!string.IsNullOrEmpty(vote.traceparent))
                        {
                            try
                            {
                                activity?.SetParentId(vote.traceparent);
                            }
                            catch (Exception ex)
                            {
                                Log.Error(ex, "Failed to set parent trace ID from Redis payload: {Traceparent}", vote.traceparent);
                            }
                        }

                        Log.ForContext("traceId", activity?.TraceId.ToString() ?? "null")
                           .ForContext("spanId", activity?.SpanId.ToString() ?? "null")
                           .Information("Processing vote for {VoteChoice} by {VoterId}", vote.vote, vote.voter_id);

                        if (!pgsql.State.Equals(System.Data.ConnectionState.Open))
                        {
                            Console.WriteLine("Reconnecting DB");
                            pgsql = OpenDbConnection("Server=db;Username=postgres;Password=postgres;");
                        }
                        else
                        {
                            // Manual span for PostgreSQL update
                            using var sqlActivity = ActivitySource.StartActivity("PostgreSQL.UpdateVote");
                            UpdateVote(pgsql, vote.voter_id, vote.vote);
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
                Console.Error.WriteLine(ex);
                return 1;
            }
        }

        private static NpgsqlConnection OpenDbConnection(string connectionString)
        {
            NpgsqlConnection connection;

            while (true)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    break;
                }
                catch
                {
                    Console.Error.WriteLine("Waiting for db");
                    Thread.Sleep(1000);
                }
            }

            Console.Error.WriteLine("Connected to db");

            var command = connection.CreateCommand();
            command.CommandText =
                @"CREATE TABLE IF NOT EXISTS votes (
                    id VARCHAR(255) NOT NULL UNIQUE,
                    vote VARCHAR(255) NOT NULL
                  )";
            command.ExecuteNonQuery();

            return connection;
        }

        private static ConnectionMultiplexer OpenRedisConnection(string hostname)
        {
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

        private static string GetIp(string hostname) =>
            Dns.GetHostEntryAsync(hostname)
               .Result
               .AddressList
               .First(a => a.AddressFamily == AddressFamily.InterNetwork)
               .ToString();

        private static void UpdateVote(NpgsqlConnection connection, string voterId, string vote)
        {
            var command = connection.CreateCommand();

            try
            {
                command.CommandText = "INSERT INTO votes (id, vote) VALUES (@id, @vote)";
                command.Parameters.AddWithValue("@id", voterId);
                command.Parameters.AddWithValue("@vote", vote);
                command.ExecuteNonQuery();
            }
            catch (DbException)
            {
                command.CommandText = "UPDATE votes SET vote = @vote WHERE id = @id";
                command.ExecuteNonQuery();
            }
            finally
            {
                command.Dispose();
            }
        }
    }
}
