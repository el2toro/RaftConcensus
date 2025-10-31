using Azure.Storage.Blobs;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Polly;
using Raft;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;

namespace RaftConcensus;

public enum NodeState { Follower, Candidate, Leader }

public class RaftNode : Raft.RaftService.RaftServiceBase
{
    private readonly string _nodeId;
    private readonly List<string> _peerAddresses;
    private readonly ILogger<RaftNode> _logger;
    private readonly BlobContainerClient _blobClient;

    //Raft State
    private NodeState _currentState = NodeState.Follower;
    private long _currentTerm = 0;
    private string _votedFor = default!;
    private List<LogEntry> _log = new();
    private long _commitIndex = 0;
    private long _lastApplied = 0;

    // Leader-specific
    private ConcurrentDictionary<string, long> _nextIndex = new();
    private ConcurrentDictionary<string, long> _matchIndex = new();

    // Timers
    private Timer _electionTimer;
    private readonly Random _random = new();
    private readonly object _lock = new();

    // State Machine (simple KV)
    private readonly ConcurrentDictionary<int, string> _stateMachine = new();

    // Persistence Paths
    private const string TERM_FILE = "term.json";
    private const string VOTE_FILE = "vote.json";
    private const string LOG_FILE = "log.json";

    public RaftNode(string nodeId, List<string> peerAddresses, string azureConnectionString, ILogger<RaftNode> logger)
    {
        _nodeId = nodeId;
        _peerAddresses = peerAddresses;
        _blobClient = new BlobContainerClient(azureConnectionString, "raft-logs");

        // Restore from disk/ Azure
        LoadPersistentState();

        // Election timeout: 150-300ms random
        ResetElectionTimer();
    }

    private void LoadPersistentState()
    {
        if (File.Exists(TERM_FILE)) _currentTerm = JsonSerializer.Deserialize<long>(File.ReadAllText(TERM_FILE));
        if (File.Exists(VOTE_FILE)) _votedFor = JsonSerializer.Deserialize<string>(File.ReadAllText(VOTE_FILE))!;
        if (File.Exists(LOG_FILE)) _log = JsonSerializer.Deserialize<List<LogEntry>>(File.ReadAllText(LOG_FILE))!;

        // Backup restore if local missing (production resilience)
        try
        {
            var blob = _blobClient.GetBlobClient($"{_nodeId}_log.json");

            if (blob.ExistsAsync().Result)
            {
                using var stream = new MemoryStream();
                blob.DownloadToAsync(stream).Wait();
                stream.Position = 0;
                _log = JsonSerializer.Deserialize<List<LogEntry>>(stream.ToArray())!;
            }

        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load from Azure");
        }
    }

    private async Task PersistStateAsync()
    {
        lock (_lock)
        {
            File.WriteAllText(TERM_FILE, JsonSerializer.Serialize(_currentTerm));
            File.WriteAllText(VOTE_FILE, JsonSerializer.Serialize(_votedFor));
            File.WriteAllText(LOG_FILE, JsonSerializer.Serialize(_log));
        }

        // Backup to Azure
        try
        {
            var blob = _blobClient.GetBlobClient($"{_nodeId}_log.json")!;
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(_log)));
            await blob.UploadAsync(stream, overwrite: true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to persist to Azure");
        }
    }

    public override Task<RequestVoteResponse> RequestVote(RequestVoteRequest request, ServerCallContext context)
    {
        lock (_lock)
        {
            _logger.LogInformation($"Received RequestVote from {request.CandidateId}, term {request.Term}");

            var response = new RequestVoteResponse { Term = _currentTerm };

            if (request.Term < _currentTerm)
                return Task.FromResult(response);

            if (request.Term > _currentTerm)
            {
                _currentTerm = request.Term;
                _votedFor = null!;
                _currentState = NodeState.Follower;

                PersistStateAsync().Wait();
            }

            bool blobUpToDate = request.LastLogTerm > _log[^1].Term ||
               (request.Term == _log[^1].Term && request.LastLogIndex >= _log.Count - 1);

            if ((_votedFor is null || _votedFor == request.CandidateId) && blobUpToDate)
            {
                _votedFor = request.CandidateId;
                response.VoteGranted = true;
                PersistStateAsync().Wait();
                ResetElectionTimer(); // Reset on granting vote
            }

            return Task.FromResult(response);
        }
    }

    private async void StartElection()
    {
        lock (_lock)
        {
            if (_currentState == NodeState.Leader) return;

            _currentState = NodeState.Candidate;
            _currentTerm++;
            _votedFor = _nodeId;

            PersistStateAsync().Wait();

            _logger.LogInformation($"Starting election for term {_currentTerm}");
        }

        int votes = 1; // Vote for self
        int majority = (_peerAddresses.Count + 1) / 2 + 1;

        foreach (var peer in _peerAddresses)
        {
            try
            {
                using var channel = GrpcChannel.ForAddress($"https://{peer}");
                var client = new Raft.RaftService.RaftServiceClient(channel);

                var request = new RequestVoteRequest
                {
                    Term = _currentTerm,
                    CandidateId = _votedFor,
                    LastLogIndex = _log.Count - 1,
                    LastLogTerm = _log.Count > 0 ? _log[^1].Term : 0
                };

                // Retry with Polly
                var response = await Policy.Handle<RpcException>()
                    .WaitAndRetryAsync(3, _ => TimeSpan.FromMilliseconds(100))
                    .ExecuteAsync(() => client.RequestVoteAsync(request).ResponseAsync);

                if (response.VoteGranted)
                {
                    lock (_lock)
                    {
                        _currentTerm = response.Term;
                        _currentState = NodeState.Follower;

                        PersistStateAsync().Wait();
                    }
                    return;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"Failed to request vote from {peer}");
            }
        }

        if (votes >= majority)
        {
            lock (_lock)
            {
                _currentState = NodeState.Leader;
                _logger.LogInformation($"Elected leader for term {_currentTerm}");

                // Initialize leader state
                foreach (var peer in _peerAddresses)
                {
                    _nextIndex[peer] = _log.Count;
                    _matchIndex[peer] = 0;
                }

                // Start heartbeats
                _ = Task.Run(SendHeartbeatsAsync);
            }
        }
        else
        {
            lock (_lock)
            {
                _currentState = NodeState.Follower;
            }
        }
        ResetElectionTimer();
    }

    public override Task<AppendEntriesResponse> AppendEntries(AppendEntriesRequest request, ServerCallContext context)
    {
        lock (_lock)
        {
            _logger.LogInformation($"Received AppendEntries from {request.LeaderId}, term {request.Term}");

            var response = new AppendEntriesResponse { Term = _currentTerm };

            if (request.Term < _currentTerm) return Task.FromResult(response);

            ResetElectionTimer(); // Reset on any leader contact

            if (request.Term > _currentTerm || _currentState == NodeState.Candidate)
            {
                _currentTerm = request.Term;
                _currentState = NodeState.Follower;
                PersistStateAsync().Wait();
            }

            // Log matching check
            if (request.PrevLogIndex > 0 && (request.PrevLogIndex >= _log.Count || _log[(int)request.PrevLogIndex].Term != request.PrevLogTerm))
            {
                return Task.FromResult(response);  // success=false
            }

            // Append new entries
            int index = (int)request.PrevLogIndex + 1;
            foreach (var entry in request.Entries)
            {
                if (index < _log.Count && _log[index].Term == entry.Term)
                {
                    index++;
                    continue;
                }
                _log = _log.GetRange(0, index);  // Truncate conflicting
                _log.Add(entry);
                index++;
            }

            PersistStateAsync().Wait();

            // Update commit
            if (request.LeaderCommit > _commitIndex)
            {
                _commitIndex = Math.Min(request.LeaderCommit, _log.Count - 1);
                ApplyCommittedEntries();
            }

            response.Success = true;
            return Task.FromResult(response);
        }
    }

    private async Task SendHeartbeatsAsync()
    {
        while (_currentState == NodeState.Leader)
        {
            foreach (var peer in _peerAddresses)
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        long next = _nextIndex.GetValueOrDefault(peer, _log.Count);
                        long prevIndex = next - 1;
                        long prevTerm = prevIndex > 0 ? _log[(int)prevIndex].Term : 0;

                        var entries = next < _log.Count ? _log.GetRange((int)next, _log.Count - (int)next) : new List<LogEntry>();

                        var request = new AppendEntriesRequest
                        {
                            Term = _currentTerm,
                            LeaderId = _nodeId,
                            PrevLogIndex = prevIndex,
                            PrevLogTerm = prevTerm,
                            LeaderCommit = _commitIndex
                        };
                        request.Entries.AddRange(entries);

                        using var channel = GrpcChannel.ForAddress($"https://{peer}");
                        var client = new Raft.RaftService.RaftServiceClient(channel);

                        var response = await Policy.Handle<RpcException>()
                            .WaitAndRetryAsync(3, _ => TimeSpan.FromMilliseconds(50))
                            .ExecuteAsync(() => client.AppendEntriesAsync(request).ResponseAsync);

                        if (response.Success)
                        {
                            _nextIndex[peer] = _log.Count;
                            _matchIndex[peer] = _log.Count - 1;
                            UpdateCommitIndex();
                        }
                        else
                        {
                            _nextIndex[peer] = Math.Max(1, _nextIndex[peer] - 1);  // Backtrack
                        }

                        if (response.Term > _currentTerm)
                        {
                            lock (_lock) { _currentTerm = response.Term; _currentState = NodeState.Follower; PersistStateAsync().Wait(); }
                        }
                    }
                    catch (Exception ex) { _logger.LogWarning(ex, $"Heartbeat to {peer} failed"); }
                });
            }

            await Task.Delay(100);  // Heartbeat interval ~100ms
        }
    }

    private void UpdateCommitIndex()
    {
        for (long n = _commitIndex + 1; n < _log.Count; n++)
        {
            int matches = 1;  // Self
            foreach (var match in _matchIndex.Values)
                if (match >= n) matches++;

            if (matches >= (_peerAddresses.Count + 1) / 2 + 1 && _log[(int)n].Term == _currentTerm)
            {
                _commitIndex = n;
                ApplyCommittedEntries();
            }
            else break;
        }
    }

    private void ApplyCommittedEntries()
    {
        while (_lastApplied < _commitIndex)
        {
            _lastApplied++;
            var entry = _log[(int)_lastApplied];
            // Apply to state machine
            var parts = entry.Command.Split(':');
            if (parts.Length == 3 && parts[0] == "set" && int.TryParse(parts[1], out int key))
            {
                _stateMachine[key] = parts[2];
            }
        }
    }

    public async Task SetValueAsync(int key, string value)
    {
        if (_currentState != NodeState.Leader) throw new InvalidOperationException("Not leader");

        var entry = new LogEntry { Term = _currentTerm, Command = $"set:{key}:{value}" };
        lock (_lock) { _log.Add(entry); PersistStateAsync().Wait(); }

        // Wait for commit (production: use semaphore or event)
        while (_commitIndex < _log.Count - 1)
            await Task.Delay(10);
    }

    public string GetValue(int key)
    {
        _stateMachine.TryGetValue(key, out var value);
        return value!;
    }

    private void ResetElectionTimer()
    {
        _electionTimer?.Dispose();
        int timeoutMs = _random.Next(150, 300);
        _electionTimer = new Timer(_ => StartElection(), null, timeoutMs, Timeout.Infinite);
    }
}
