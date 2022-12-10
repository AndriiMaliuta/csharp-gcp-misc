using Google.Cloud.PubSub.V1;
using Google.Cloud.Datastore.V1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;


public class PublishMessagesAsyncSample
{
    public static void Main()
    {
        Console.WriteLine(">>> start...");
        var projectId = "silver-adapter-307718";

        var message = CreateDbRecord(projectId);
        var messages = new List<string>() { message };
        var count = PublishMessagesAsync(projectId: projectId, "wiki-pages-main", messages);
        Console.WriteLine(count.Result.ToString());
    }

    private static async Task<int> PublishMessagesAsync(string projectId, string topicId,
        IEnumerable<string> messageTexts)
    {
        Console.WriteLine("...");
        TopicName topicName = TopicName.FromProjectTopic(projectId, topicId);
        PublisherClient publisher = await PublisherClient.CreateAsync(topicName);

        int publishedMessageCount = 0;
        var publishTasks = messageTexts.Select(async text =>
        {
            try
            {
                string message = await publisher.PublishAsync(text);
                Console.WriteLine($"Published message {message}");
                Interlocked.Increment(ref publishedMessageCount);
            }
            catch (Exception exception)
            {
                Console.WriteLine($"An error occurred when publishing message {text}: {exception.Message}");
            }
        });
        await Task.WhenAll(publishTasks);
        return publishedMessageCount;
    }

    private static string CreateDbRecord(string projId)
    {
        DatastoreDb db = DatastoreDb.Create(projId);
        var kind = "Cat1";
        const string name = "Simba";
        KeyFactory keyFactory = db.CreateKeyFactory(kind);
        Key key = keyFactory.CreateKey(name);
        const int age = 7;
        var cat = new Entity
        {
            Key = key,
            ["age"] = age,
            ["name"] = name
        };

        using DatastoreTransaction transaction = db.BeginTransaction();
        transaction.Upsert(cat);
        transaction.Commit();
        return $"Saved cat {name} :: {age}";
    }
}