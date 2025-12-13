using System;
using System.Data.Common;
using System.Diagnostics; // Required for Activity and ActivityContext
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Newtonsoft.Json;
using Npgsql;
using Serilog;
using Serilog.Formatting.Json;
using StackExchange.Redis;

namespace Worker
{
    public class Program
    {
        public static int Main(string[] args)
        {
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

                        // -------------------------------
                        // Start worker Activity (Span)
                        // -------------------------------
                        using (var activity = new Activity("Worker.ProcessVote"))
                        {
                            // --- FINAL CRITICAL FIX: Direct W3C Header Injection ---
                            if (!string.IsNullOrEmpty(vote.traceparent))
                            {
                                try 
                                {
                                    // SetParentId(string) attempts to parse the W3C traceparent header 
                                    // and set the current Activity's TraceId and ParentSpanId.
                                    activity.SetParentId(vote.traceparent);
                                }
                                catch (Exception ex)
                                {
                                     // If linking fails (e.g., malformed header), log the failure.
                                     // The activity will proceed and start a new root trace, 
                                     // which is better than crashing.
                                     Log.Error(ex, "Failed to set parent trace ID from Redis payload: {Traceparent}", vote.traceparent);
                                }
                            }
                            // ---------------------------------------------------------
                            
                            activity.Start(); // Start the span.

                            // Log context - The TraceId here MUST now match the Python Trace ID.
                            Log.ForContext("traceId", activity.TraceId.ToString())
                               .ForContext("spanId", activity.SpanId.ToString())
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
                            
                            activity.Stop(); // Stop the span.
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

        private static void UpdateVote(
            NpgsqlConnection connection,
            string voterId,
            string vote)
        {
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