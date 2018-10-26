package pubsub;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

public class PubSubStubFormatRunner {

    private static final String HOST = "localhost:8085";
    private static final LocalhostPubSubService pubSubService = new LocalhostPubSubService(HOST);
    private static final String PROJECT_ID = "dataflow-report-test";

    private static final String TOPIC_FORMAT = "format";
    private static final String TOPIC_TO_BQ = "to_bq";
    private static final String TOPIC_SUBSCRIPTION = "projects/" + PROJECT_ID + "/topics/format";
    private static final String SUBSCRIPTION = "projects/" + PROJECT_ID + "/subscriptions/format";


    public static void main(String[] args) throws Exception {
        // For init (only once)
        //pubSubService.createTopic(PROJECT_ID, TOPIC_FORMAT);
        //pubSubService.createTopic(PROJECT_ID, TOPIC_TO_BQ);
        //pubSubService.createPullSubscription(TOPIC_SUBSCRIPTION, SUBSCRIPTION);

        sendMessages(PROJECT_ID, TOPIC_FORMAT);
        // Send 5000 messages every 2 sec
        //for(int i=0; i<15; i++) {
        //sendMessages(PROJECT_ID, TOPIC_FOO, 5000);
        //    Thread.sleep(2000);
        //}

    }

    public static void sendMessages(String projectId, String topic) throws Exception {
        ByteString data = ByteString.copyFromUtf8("[{\n" +
                " \"kind\": \"drive#fileList\",\n" +
                " \"nextPageToken\": \"~!!~AI9FV7SKhVD5n4PUgD6fS0ARCnMjxhE15Cxln9AEjQIPHzy3v7-4ZnslUkjRzCc2FerX18UYIWufSbOTRg1KzL8LG4U8M8H0ubsMqpIE4gb839AaHEXhtPBVUa7Fe-ecLTdGoWZ5uaXhxWQ53EgmebaOoPOKi0F0nMcAb-B9GKpxoHzcEaqtyQ6yLQh_VBfVUcIuFJJdLLFoQguEccOs0iTxZ6jvL3oDkTrE271RFulDLIs4MmH18mHjb_cdd_GTicRrYbqTolzb3Py3jFQcf5HkQr-6qzlryGeinRX7LzRMmTrrgycTjJ2Xu_8n-nwxwINlg8_NnKMo1JZ4zhiIvTB9Nzc0WzPg2pQbfS0M5KoaDwLlav0aft3kI4S-ipggj7L5wf8TVK9j\",\n" +
                " \"incompleteSearch\": false,\n" +
                " \"files\": [\n" +
                "  {\n" +
                "   \"kind\": \"drive#file\",\n" +
                "   \"id\": \"1ofLFZNlfqJ8iDxA_i5AtITgtVOnLr1hIIXgouhbnkPA\",\n" +
                "   \"name\": \"Trainer Report [Active]\",\n" +
                "   \"mimeType\": \"application/vnd.google-apps.spreadsheet\",\n" +
                "   \"description\": \"aodocs_imported\\naodocs_target_domain_revevol.eu\\naodocs_target_library_OobHkv83HOFnzcIaID\\naodocs_target_document_id_QgVxd5o9zmErszq7RV\\naodocs_target_domain_revevol.eu\\naodocs_target_library_OobHkv83HOFnzcIaID\\naodocs_target_document_id_QgVyuvi3qlbWboAOXA\",\n" +
                "   \"starred\": false,\n" +
                "   \"trashed\": false,\n" +
                "   \"explicitlyTrashed\": false,\n" +
                "   \"parents\": [\n" +
                "    \"0B9INzslaeeu8UmtISE9ib2lwMmc\"\n" +
                "   ],\n" +
                "   \"spaces\": [\n" +
                "    \"drive\"\n" +
                "   ],\n" +
                "   \"version\": \"1869147\",\n" +
                "   \"webViewLink\": \"https://docs.google.com/spreadsheets/d/1ofLFZNlfqJ8iDxA_i5AtITgtVOnLr1hIIXgouhbnkPA/edit?usp=drivesdk\",\n" +
                "   \"iconLink\": \"https://drive-thirdparty.googleusercontent.com/16/type/application/vnd.google-apps.spreadsheet\",\n" +
                "   \"hasThumbnail\": true,\n" +
                "   \"thumbnailLink\": \"https://docs.google.com/a/revevol.eu/feeds/vt?gd=true&id=1ofLFZNlfqJ8iDxA_i5AtITgtVOnLr1hIIXgouhbnkPA&v=10122&s=AMedNnoAAAAAW9HwgwWWgV6svICHWJYN788JRHLD9gdI&sz=s220\",\n" +
                "   \"thumbnailVersion\": \"10122\",\n" +
                "   \"viewedByMe\": false,\n" +
                "   \"createdTime\": \"2013-12-23T13:06:33.016Z\",\n" +
                "   \"modifiedTime\": \"2018-10-25T13:32:47.425Z\",\n" +
                "   \"modifiedByMe\": false,\n" +
                "   \"owners\": [\n" +
                "    {\n" +
                "     \"kind\": \"drive#user\",\n" +
                "     \"displayName\": \"Training Revevol\",\n" +
                "     \"me\": false,\n" +
                "     \"permissionId\": \"07440503860394615925\",\n" +
                "     \"emailAddress\": \"training@academy.revevol.eu\"\n" +
                "    }\n" +
                "   ],\n" +
                "   \"shared\": true,\n" +
                "   \"ownedByMe\": false,\n" +
                "   \"capabilities\": {\n" +
                "    \"canAddChildren\": false,\n" +
                "    \"canChangeCopyRequiresWriterPermission\": false,\n" +
                "    \"canChangeViewersCanCopyContent\": false,\n" +
                "    \"canComment\": false,\n" +
                "    \"canCopy\": true,\n" +
                "    \"canDelete\": false,\n" +
                "    \"canDownload\": true,\n" +
                "    \"canEdit\": false,\n" +
                "    \"canListChildren\": false,\n" +
                "    \"canMoveItemIntoTeamDrive\": false,\n" +
                "    \"canReadRevisions\": false,\n" +
                "    \"canRemoveChildren\": false,\n" +
                "    \"canRename\": false,\n" +
                "    \"canShare\": false,\n" +
                "    \"canTrash\": false,\n" +
                "    \"canUntrash\": false\n" +
                "   },\n" +
                "   \"viewersCanCopyContent\": true,\n" +
                "   \"copyRequiresWriterPermission\": false,\n" +
                "   \"writersCanShare\": true,\n" +
                "   \"quotaBytesUsed\": \"0\",\n" +
                "   \"isAppAuthorized\": false\n" +
                "  },\n" +
                "  {\n" +
                "   \"kind\": \"drive#file\",\n" +
                "   \"id\": \"1DjrTWOQkmlJRFWeQgAz1icITXQ3kf9g4QvBqPi80GTQ\",\n" +
                "   \"name\": \"[SMCP] - Step 2_Formation Complémentaire Pilote Teams\",\n" +
                "   \"mimeType\": \"application/vnd.google-apps.spreadsheet\",\n" +
                "   \"description\": \"aodocs_domain_revevol.eu\\naodocs_library_R21Q3Vt1UBHLp7K7GJ\\naodocs_title_SMCP - Complément de formation teams\\naodocs_document_id_R7DrsjHx4Dw4HKTec4\\naodocs_folder_1vC36mhxOih9ZK7YTLv7-nEx3XyFyizia\\n\",\n" +
                "   \"starred\": false,\n" +
                "   \"trashed\": false,\n" +
                "   \"explicitlyTrashed\": false,\n" +
                "   \"spaces\": [\n" +
                "    \"drive\"\n" +
                "   ],\n" +
                "   \"version\": \"45\",\n" +
                "   \"webContentLink\": \"https://drive.google.com/a/revevol.eu/uc?id=1DjrTWOQkmlJRFWeQgAz1icITXQ3kf9g4QvBqPi80GTQ&export=download\",\n" +
                "   \"webViewLink\": \"https://docs.google.com/spreadsheets/d/1DjrTWOQkmlJRFWeQgAz1icITXQ3kf9g4QvBqPi80GTQ/edit?usp=drivesdk\",\n" +
                "   \"iconLink\": \"https://drive-thirdparty.googleusercontent.com/16/type/application/vnd.google-apps.spreadsheet\",\n" +
                "   \"hasThumbnail\": true,\n" +
                "   \"thumbnailLink\": \"https://docs.google.com/a/revevol.eu/feeds/vt?gd=true&id=1DjrTWOQkmlJRFWeQgAz1icITXQ3kf9g4QvBqPi80GTQ&v=12&s=AMedNnoAAAAAW9Hwg8CRnTN6KXyMoM7K5FV_woB59xdg&sz=s220\",\n" +
                "   \"thumbnailVersion\": \"12\",\n" +
                "   \"viewedByMe\": false,\n" +
                "   \"createdTime\": \"2018-10-22T16:03:35.330Z\",\n" +
                "   \"modifiedTime\": \"2018-10-25T13:23:13.196Z\",\n" +
                "   \"modifiedByMe\": false,\n" +
                "   \"sharedWithMeTime\": \"2018-10-22T16:03:48.519Z\",\n" +
                "   \"sharingUser\": {\n" +
                "    \"kind\": \"drive#user\",\n" +
                "    \"displayName\": \"Docs Revevol\",\n" +
                "    \"me\": false,\n" +
                "    \"permissionId\": \"14084381648230197577\",\n" +
                "    \"emailAddress\": \"revevol@revevol.eu\"\n" +
                "   },\n" +
                "   \"owners\": [\n" +
                "    {\n" +
                "     \"kind\": \"drive#user\",\n" +
                "     \"displayName\": \"Docs Revevol\",\n" +
                "     \"me\": false,\n" +
                "     \"permissionId\": \"14084381648230197577\",\n" +
                "     \"emailAddress\": \"revevol@revevol.eu\"\n" +
                "    }\n" +
                "   ],\n" +
                "   \"lastModifyingUser\": {\n" +
                "    \"kind\": \"drive#user\",\n" +
                "    \"displayName\": \"Raphaël Dozolme\",\n" +
                "    \"photoLink\": \"https://lh5.googleusercontent.com/-fbOWlxsxFcI/AAAAAAAAAAI/AAAAAAAAAA8/Tlo2Pw9VkNw/s64/photo.jpg\",\n" +
                "    \"me\": false,\n" +
                "    \"permissionId\": \"04575654338453376152\",\n" +
                "    \"emailAddress\": \"raphael.dozolme@revevol.eu\"\n" +
                "   },\n" +
                "   \"shared\": true,\n" +
                "   \"ownedByMe\": false,\n" +
                "   \"capabilities\": {\n" +
                "    \"canAddChildren\": false,\n" +
                "    \"canChangeCopyRequiresWriterPermission\": false,\n" +
                "    \"canChangeViewersCanCopyContent\": false,\n" +
                "    \"canComment\": true,\n" +
                "    \"canCopy\": true,\n" +
                "    \"canDelete\": false,\n" +
                "    \"canDownload\": true,\n" +
                "    \"canEdit\": true,\n" +
                "    \"canListChildren\": false,\n" +
                "    \"canMoveItemIntoTeamDrive\": false,\n" +
                "    \"canReadRevisions\": true,\n" +
                "    \"canRemoveChildren\": false,\n" +
                "    \"canRename\": true,\n" +
                "    \"canShare\": false,\n" +
                "    \"canTrash\": false,\n" +
                "    \"canUntrash\": false\n" +
                "   },\n" +
                "   \"viewersCanCopyContent\": true,\n" +
                "   \"copyRequiresWriterPermission\": false,\n" +
                "   \"writersCanShare\": false,\n" +
                "   \"quotaBytesUsed\": \"0\",\n" +
                "   \"isAppAuthorized\": false\n" +
                "  }\n" +
                " ]\n" +
                "}]");
        pubSubService.publish(projectId, topic, PubsubMessage.newBuilder().setData(data).build());
    }

}
