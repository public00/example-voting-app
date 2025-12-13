using System;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting; // Required for Host.CreateDefaultBuilder
using Newtonsoft.Json;
using Npgsql;
using Serilog;
using Serilog.Formatting.Json;
using StackExchange.Redis;
using OpenTelemetry;
using OpenTelemetry.Trace;
using OpenTelemetry.Resources;
using OpenTelemetry.Context.Propagation; // Required for TraceContextPropagator
using OpenTelemetry.Instrumentation.StackExchangeRedis; // Required for Redis instrumentation

namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
            // ---- Host + DI + OpenTelemetry Setup ----
            // We use the Host builder pattern to correctly initialize OpenTelemetry 
            // and make the Tracer available via Dependency Injection.
            using var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices(services =>
                {
                    services.AddOpenTelemetry()
                        .WithTracing(builder => builder
                            // Add a source name, used when retrieving the tracer
                            .AddSource("Worker")
                            // Automatically trace StackExchange.Redis operations
                            .AddRedisInstrumentation() 
                            .SetResourceBuilder(
                                ResourceBuilder.CreateDefault()
                                    .AddService("Worker")
                            )
                            // NOTE: OTLP Exporter is redundant if using Dynatrace OneAgent, 
                            // but included here for a complete, exportable OTEL configuration.
                            .AddOtlpExporter(opt =>
                            {
                                // Placeholder endpoint and headers for demonstration
                                opt.Endpoint = new Uri("https://<your-env>.live.dynatrace.com/api/v2/otlp");
                                opt.Headers = "Authorization=Api-Token <token>";
                            })
                        );
                })
                .Build();
                
            // Retrieve the tracer after the host is built
            var tracer = host.Services.GetRequiredService<TracerProvider>().GetTracer("Worker");


            // ---- Serilog setup ----
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console(new JsonFormatter())
                .CreateLogger();

            try
            {
                // ---- Worker infrastructure ----
                var pgsql = OpenDbConnection("Server=db;Username=postgres;Password=postgres;");
                var redisConn = OpenRedisConnection("redis");
                var redis = redisConn.GetDatabase();

                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                // Add 'traceparent' to the definition
                var definition = new { vote = "", voter_id = "", traceparent = "" };

                // ---- Main worker loop ----
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

                        // ---- Extract parent trace context using W3C standard ----
                        ActivityContext parentContext = default;
                        bool hasParent = false;

                        if (!string.IsNullOrEmpty(vote.traceparent))
                        {
                            // 1. Use the standard OTEL W3C TraceContextPropagator to extract the parent context
                            var propagator = new TraceContextPropagator();
                            
                            // The Python app put the full W3C string in the 'traceparent' field, 
                            // so we provide a delegate that returns the value for the 'traceparent' key.
                            var context = propagator.Extract(default, vote.traceparent, 
                                (carrier, key) => key == "traceparent" ? new[] { carrier } : Enumerable.Empty<string>());
                            
                            if (context.ActivityContext.HasValue)
                            {
                                parentContext = context.ActivityContext.Value;
                                hasParent = true;
                            }
                        }

                        // ---- Create Worker Activity with correct parent ----
                        
                        // Use the tracer to start a new Activity (Span)
                        using (var activity = tracer.StartActivity(
                            "Worker.ProcessVote",
                            ActivityKind.Consumer,
                            hasParent ? parentContext : default))
                        {
                            // Add logging fields
                            // The Dynatrace OneAgent automatically reports the Span/Trace ID, 
                            // but we log it here for robust correlation with Serilog.
                            Log.ForContext("traceId", activity?.TraceId.ToString())
                               .ForContext("spanId", activity?.SpanId.ToString())
                               .Information("Processing vote for {VoteChoice} by {VoterId}",
                                    vote.vote, vote.voter_id);

                            // ---- DB Logic ----
                            if (!pgsql.State.Equals(System.Data.ConnectionState.Open))
                            {
                                Console.WriteLine("Reconnecting DB");
                                pgsql = OpenDbConnection("Server=db;Username=postgres;Password=postgres;");
                            }
                            else
                            {
                                UpdateVote(pgsql, vote.voter_id, vote.vote);
                            }
                            
                            // Stop the activity when done processing this specific vote
                            activity.Stop();
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

        // ----------------------------------------
        // Helpers
        // ----------------------------------------

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