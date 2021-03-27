const AWS = require("aws-sdk");

const ddb = new AWS.DynamoDB.DocumentClient({
  apiVersion: "2012-08-10",
  region: process.env.AWS_REGION,
});

const { TABLE_NAME } = process.env;

exports.handler = async (event) => {
  const connectionId = event.requestContext.connectionId;
  const { roomId } = JSON.parse(event.body);

  try {
    await ddb
      .put({
        TableName: TABLE_NAME,
        Item: {
          PK: `RoomId#${roomId}`,
          SK: `ConnectionId#${connectionId}`,
          connectionId,
        },
      })
      .promise();
  } catch (e) {
    return { statusCode: 500, body: e.stack };
  }

  let connectionData;
  try {
    connectionData = await ddb
      .scan({
        TableName: TABLE_NAME,
        ProjectionExpression: "PK, SK, connectionId",
        FilterExpression: "PK = :pk AND begins_with(SK, :skprefix)",
        ExpressionAttributeValues: {
          ":pk": `RoomId#${roomId}`,
          ":skprefix": "ConnectionId#",
        },
      })
      .promise();
  } catch (e) {
    return { statusCode: 500, body: e.stack };
  }

  const apigwManagementApi = new AWS.ApiGatewayManagementApi({
    apiVersion: "2018-11-29",
    endpoint:
      event.requestContext.domainName + "/" + event.requestContext.stage,
  });

  const updateCalls = connectionData.Items.map(
    async ({ connectionId, PK, SK }) => {
      try {
        await apigwManagementApi
          .postToConnection({
            ConnectionId: connectionId,
            Data: JSON.stringify({
              room: {
                users: connectionData.Items.map((connectionId) => connectionId),
              },
            }),
          })
          .promise();
      } catch (e) {
        if (e.statusCode === 410) {
          console.log(`Found stale connection, deleting ${connectionId}`);
          await ddb
            .delete({
              TableName: TABLE_NAME,
              Key: {
                PK: `ConnectionId#${event.requestContext.connectionId}`,
                SK: "Metadata",
              },
            })
            .promise();
        } else {
          throw e;
        }
      }
    }
  );

  try {
    await Promise.all(updateCalls);
  } catch (e) {
    return { statusCode: 500, body: e.stack };
  }

  return { statusCode: 200, body: "Data sent." };
};
