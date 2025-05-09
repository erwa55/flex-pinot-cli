#!/usr/bin/env groovy

@Grab('com.opencsv:opencsv:5.5.2')
@Grab('commons-cli:commons-cli:1.5.0')
@Grab('org.slf4j:slf4j-simple:1.7.30')

import com.opencsv.CSVReader
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.commons.cli.*
import java.nio.file.Files
import java.nio.file.Paths
import java.io.Console
import java.io.FileReader
import java.util.logging.Logger
import java.util.logging.Level

/**
 * Flex Pinot CLI Tool
 * 
 * A command-line tool for creating and managing resources in Flex from CSV data.
 * Supports storage, folder, and inbox resource types with proper configuration.
 */
class FlexPinotCli {
    static final String VERSION = "1.0.0"
    static final Logger logger = Logger.getLogger("FlexPinotCli")
    
    // Resource type constants
    static final String TYPE_FOLDER = "folder"
    static final String TYPE_INBOX = "inbox"
    static final String TYPE_STORAGE = "storage"
    
    // Plugin class mappings
    static final Map<String, String> PLUGIN_CLASSES = [
        (TYPE_FOLDER): "tv.nativ.mio.enterprise.resources.impl.capacity.folder.MioFolderResource",
        (TYPE_INBOX): "tv.nativ.mio.enterprise.resources.impl.capacity.folder.inbox.InboxResource",
        (TYPE_STORAGE): "tv.nativ.mio.enterprise.resources.impl.capacity.storage.vfs.VFSStorageResource"
    ]

    // Resource type emojis for pretty printing
    static final Map<String, String> TYPE_EMOJIS = [
        (TYPE_FOLDER): "📂",
        (TYPE_INBOX): "📥",
        (TYPE_STORAGE): "📀"
    ]
    
    String apiBaseUrl
    String username
    String password
    String basicAuth
    boolean dryRun = false
    boolean verbose = false
    boolean skipValidation = false
    String accountId
    Map<String, String> storageIdMap = [:]
    JsonSlurper jsonSlurper = new JsonSlurper()
    
    // Cache of existing resources to avoid duplicate API calls
    Map<String, Boolean> existingResourcesCache = [:]
    Map<String, Boolean> existingWorkflowCache = [:]
    Map<String, Boolean> existingUserCache = [:]
    Map<String, Boolean> existingMetadataCache = [:]
    
    /**
     * Main entry point
     */
    static void main(String[] args) {
        FlexPinotCli cli = new FlexPinotCli()
        cli.parseAndRun(args)
    }
    
    /**
     * Parse command line arguments and run appropriate commands
     */
    void parseAndRun(String[] args) {
        Options options = new Options()
        
        options.addOption(Option.builder("h")
            .longOpt("help")
            .desc("Show help")
            .build())
            
        options.addOption(Option.builder("v")
            .longOpt("version")
            .desc("Show version")
            .build())
            
        options.addOption(Option.builder("u")
            .longOpt("url")
            .hasArg()
            .argName("URL")
            .desc("API base URL (e.g., https://medias.flex.daletdemos.com)")
            .build())
            
        options.addOption(Option.builder("U")
            .longOpt("username")
            .hasArg()
            .argName("USERNAME")
            .desc("Flex username")
            .build())
            
        options.addOption(Option.builder("P")
            .longOpt("password")
            .hasArg()
            .argName("PASSWORD")
            .desc("Flex password (warning: using this flag exposes password in command history)")
            .build())
            
        options.addOption(Option.builder("d")
            .longOpt("dry-run")
            .desc("Validate but don't execute API calls")
            .build())
            
        options.addOption(Option.builder("V")
            .longOpt("verbose")
            .desc("Enable verbose output")
            .build())
            
        options.addOption(Option.builder("s")
            .longOpt("skip-validation")
            .desc("Skip validation of existing resources and dependencies")
            .build())
            
        options.addOption(Option.builder("f")
            .longOpt("force")
            .desc("Force creation even if resources already exist")
            .build())

        CommandLineParser parser = new DefaultParser()
        HelpFormatter formatter = new HelpFormatter()
        
        try {
            CommandLine cmd = parser.parse(options, args)
            
            if (cmd.hasOption("help")) {
                printHelp(formatter, options)
                return
            }
            
            if (cmd.hasOption("version")) {
                println "Flex Pinot CLI v${VERSION}"
                return
            }
            
            List<String> positionalArgs = cmd.getArgList()
            if (positionalArgs.isEmpty()) {
                println "❌ Missing CSV file path"
                printHelp(formatter, options)
                System.exit(1)
            }
            
            String csvFilePath = positionalArgs[0]
            if (!Files.exists(Paths.get(csvFilePath))) {
                println "❌ CSV file not found: ${csvFilePath}"
                System.exit(1)
            }
            
            // Set options
            this.verbose = cmd.hasOption("verbose")
            this.dryRun = cmd.hasOption("dry-run")
            this.skipValidation = cmd.hasOption("skip-validation")
            this.forceCreation = cmd.hasOption("force")
            
            // Get credentials
            Console console = System.console()
            if (!console && (!cmd.hasOption("username") || !cmd.hasOption("password") || !cmd.hasOption("url"))) {
                println "❌ This script must be run from a terminal to accept secure password input,"
                println "   or all credentials must be provided via command-line options."
                System.exit(1)
            }
            
            this.apiBaseUrl = cmd.getOptionValue("url") ?: console.readLine("🌐 Enter API base URL (e.g., https://medias.flex.daletdemos.com): ").trim()
            this.username = cmd.getOptionValue("username") ?: console.readLine("👤 Enter Flex username: ").trim()
            
            if (cmd.hasOption("password")) {
                this.password = cmd.getOptionValue("password")
            } else {
                this.password = new String(console.readPassword("🔐 Enter Flex password: ")).trim()
            }
            
            this.basicAuth = "${username}:${password}".bytes.encodeBase64().toString()
            
            // Run the import process
            if (this.dryRun) {
                println "🔍 DRY RUN MODE: No API calls will be executed"
            }
            
            println """
            ______ _            ______ _             _   
            |  ___| |           | ___ (_)           | |  
            | |_  | | _____  __ | |_/ /_ _ __   ___ | |_ 
            |  _| | |/ _ \\ \\/ / |  __/| | '_ \\ / _ \\| __|
            | |   | |  __/>  <  | |   | | | | | (_) | |_ 
            \\_|   |_|\\___/_/\\_\\ \\_|   |_|_| |_|\\___/ \\__|
                 
                  F L E X   P I N O T   🍷          
         Pouring media ressources with flavor
                                                        
            🚀 Starting Flex Pinot CLI v${VERSION}
            """

            println "🔌 Connecting to: ${apiBaseUrl}"
            
            // Discover account ID
            if (!this.dryRun) {
                fetchAccountId()
            } else {
                this.accountId = "dry-run-account-id"
                println "🔍 Using placeholder Account ID for dry run: ${accountId}"
            }
            
            // Process CSV file
            processCSV(csvFilePath)
            
            println "\n✅ Operation completed successfully"
            
        } catch (ParseException e) {
            println "❌ Error parsing command-line arguments: ${e.message}"
            printHelp(formatter, options)
            System.exit(1)
        } catch (Exception e) {
            println "❌ Error: ${e.message}"
            if (verbose) {
                e.printStackTrace()
            }
            System.exit(1)
        }
    }
    
    /**
     * Print help information
     */
    void printHelp(HelpFormatter formatter, Options options) {
        formatter.printHelp("groovy flex-pinot-cli.groovy [options] <path-to-csv>", 
            "\nFlex Pinot CLI Tool v${VERSION}\n" +
            "Creates resources in Flex from CSV data\n\n" +
            "Options:", options,
            "\nCSV Format Requirements:\n" +
            "  - Headers must include: Type, Ref\n" +
            "  - For storage: Protocol, Hostname, Bucket, Path, Key, Secret, Shard\n" +
            "  - For folder/inbox: Link to (references storage Ref)\n" +
            "  - For inbox: Optional WorkflowID, WorkflowOwner, InboxMetadata\n" +
            "  - Tags column can contain comma-separated tags\n\n" +
            "Validation:\n" +
            "  - By default, the tool validates resources don't already exist\n" +
            "  - For inboxes, it validates that WorkflowID, WorkflowOwner, and InboxMetadata exist\n" +
            "  - Use --skip-validation to bypass these checks\n" +
            "  - Use --force to create resources even if they already exist\n\n" +
            "Examples:\n" +
            "  groovy flex-pinot-cli.groovy -u https://flex.example.com -U admin resources.csv\n" +
            "  groovy flex-pinot-cli.groovy --dry-run --verbose resources.csv\n" +
            "  groovy flex-pinot-cli.groovy --force --skip-validation -u https://flex.example.com resources.csv\n", 
            true)
    }
    
    /**
     * Fetch account ID from API
     */
    void fetchAccountId() {
        log("Discovering account ID...")
        
        try {
            def accountConn = createConnection("${apiBaseUrl}/api/accounts", "GET")
            def (accountRespCode, accountRespText) = executeRequest(accountConn)
            
            if (accountRespCode < 300) {
                def accounts = jsonSlurper.parseText(accountRespText)?.accounts
                accountId = accounts?.getAt(0)?.id
                if (!accountId) throw new Exception("No account ID found in response.")
                println "🔍 Auto-discovered Account ID: $accountId"
            } else {
                throw new Exception("Failed to retrieve account list: ${accountRespCode} - $accountRespText")
            }
        } catch (Exception e) {
            throw new Exception("Error while fetching account ID: ${e.message}", e)
        }
    }
    
    /**
     * Process the CSV file and create resources
     */
    void processCSV(String csvFilePath) {
        log("Processing CSV file: ${csvFilePath}")
        
        try {
            def reader = new CSVReader(new FileReader(csvFilePath))
            def rows = reader.readAll()
            def headers = rows[0].collect { it.trim() }
            def lines = rows.drop(1)
            
            // Validate required headers
            validateHeaders(headers)
            
            // First pass - create all storage resources
            println "\n📊 First pass: Creating storage resources..."
            processResourcesByType(headers, lines, TYPE_STORAGE)
            
            // Second pass - create folders (which depend on storage)
            println "\n📊 Second pass: Creating folder resources..."
            processResourcesByType(headers, lines, TYPE_FOLDER)
            
            // Third pass - create inboxes (which depend on storage)
            println "\n📊 Third pass: Creating inbox resources..."
            processResourcesByType(headers, lines, TYPE_INBOX)
            
        } catch (Exception e) {
            throw new Exception("Error processing CSV: ${e.message}", e)
        }
    }
    
    /**
     * Validate the CSV headers contain required fields
     */
    void validateHeaders(List<String> headers) {
        def requiredHeaders = ["Type", "Ref"]
        def missingHeaders = requiredHeaders.findAll { !headers.contains(it) }
        
        if (missingHeaders) {
            throw new Exception("Missing required CSV headers: ${missingHeaders.join(', ')}")
        }
    }
    
    /**
     * Process resources of a specific type
     */
    void processResourcesByType(List<String> headers, List<List<String>> lines, String targetType) {
        lines.eachWithIndex { values, lineIndex ->
            def row = [:]
            headers.eachWithIndex { h, i ->
                row[h] = (i < values.length) ? values[i]?.trim() : ""
            }
            
            def type = row["Type"]?.toLowerCase()
            def ref = row["Ref"]
            
            // Skip if not the target type or missing required fields
            if (type != targetType) return
            if (!ref) {
                println "\n⚠️  Skipping row ${lineIndex + 2}: Missing 'Ref' value"
                return
            }
            
            createResource(type, ref, row)
        }
    }
    
    /**
     * Create a resource in Flex
     */
    void createResource(String type, String ref, Map<String, String> row) {
        def emoji = TYPE_EMOJIS[type] ?: "🔹"
        println "\n${emoji} Creating ${type.capitalize()}: $ref"
        
        def pluginClass = PLUGIN_CLASSES[type]
        if (!pluginClass) {
            println "❌ Unknown resource type: ${type}"
            return
        }
        
        // Check if resource already exists
        if (!skipValidation && !dryRun && !forceCreation) {
            if (resourceExists(ref)) {
                println "⚠️ Resource with name '${ref}' already exists. Use --force to create anyway."
                return
            }
        }
        
        // Validate type-specific requirements
        if (type in [TYPE_FOLDER, TYPE_INBOX]) {
            def linkedRef = row["Link to"]
            if (!linkedRef) {
                println "❌ Missing 'Link to' field for ${type}: ${ref}"
                return
            }
            if (!dryRun && !storageIdMap.containsKey(linkedRef)) {
                println "❌ Referenced storage '${linkedRef}' not found"
                return
            }
        }
        
        // Validate inbox-specific dependencies
        if (type == TYPE_INBOX && !skipValidation && !dryRun) {
            // Validate WorkflowID if provided
            if (row["WorkflowID"] && !workflowExists(row["WorkflowID"])) {
                println "❌ Workflow ID '${row["WorkflowID"]}' not found"
                return
            }
            
            // Validate WorkflowOwner if provided
            if (row["WorkflowOwner"] && !userExists(row["WorkflowOwner"])) {
                println "❌ User ID '${row["WorkflowOwner"]}' not found"
                return
            }
            
            // Validate InboxMetadata if provided
            if (row["InboxMetadata"] && !metadataExists(row["InboxMetadata"])) {
                println "❌ Metadata Definition ID '${row["InboxMetadata"]}' not found"
                return
            }
        }
        
        if (dryRun) {
            println "🔍 DRY RUN: Would create ${type} resource with ref: ${ref}"
            if (type == TYPE_STORAGE) {
                storageIdMap[ref] = "dry-run-storage-id-${ref}"
            }
            return
        }
        
        // Create the resource
        def createPayload = new JsonBuilder([
            description: row["Description"] ?: "Auto-created from CSV",
            name: ref,
            pluginClass: pluginClass,
            pollingInterval: row["PollingInterval"] ? row["PollingInterval"] as Integer : 30,
            useLatestAvailableVersion: true,
            visibilityIds: [accountId]
        ]).toPrettyString()
        
        log("Creating resource payload: ${createPayload}")
        
        def createConn = createConnection("${apiBaseUrl}/api/resources", "POST", createPayload)
        def (createRespCode, createRespText) = executeRequest(createConn)
        
        if (createRespCode >= 200 && createRespCode < 300) {
            def resourceId = jsonSlurper.parseText(createRespText)?.id
            if (!resourceId) {
                println "❌ Could not extract ID for $ref"
                return
            }
            
            configureResource(type, ref, row, resourceId)
            applyTags(row, resourceId)
            enableResource(resourceId)
            
            // Store storage IDs for later linking
            if (type == TYPE_STORAGE) {
                storageIdMap[ref] = resourceId
                log("Added storage ID mapping: ${ref} -> ${resourceId}")
            }
        } else {
            println "❌ Create failed (${createRespCode}): ${createRespText}"
        }
    }
    
    /**
     * Configure a resource with type-specific settings
     */
    void configureResource(String type, String ref, Map<String, String> row, String resourceId) {
        def configPayload
        
        switch (type) {
            case TYPE_STORAGE:
                configPayload = new JsonBuilder([
                    "vfs-location": [
                        protocol : row["Protocol"],
                        bucket   : row["Bucket"],
                        hostname : row["Hostname"],
                        path     : row["Path"],
                        key      : row["Key"],
                        secret   : row["Secret"],
                        sharded  : row["Shard"]?.toLowerCase() in ['yes', 'true', '1']
                    ]
                ]).toPrettyString()
                break
                
            case TYPE_FOLDER:
                def linkedRef = row["Link to"]
                def storageId = storageIdMap[linkedRef]
                
                configPayload = new JsonBuilder([
                    "Storage Resources": [["Storage Resource": [id: storageId]]]
                ]).toPrettyString()
                break
                
            case TYPE_INBOX:
                def linkedRef = row["Link to"]
                def storageId = storageIdMap[linkedRef]
                
                def config = [
                    "housekeeping-period": row["HousekeepingPeriod"] ? row["HousekeepingPeriod"] as Long : 86400000000,
                    "storage-resources": ["Storage Resource": [[id: storageId]]],
                    "plugins": [["plugin-name": "mioMetadataForm"]]
                ]
                
                if (row["WorkflowID"]) config["workflow"] = [id: row["WorkflowID"] as Integer]
                if (row["WorkflowOwner"]) config["workflowOwner"] = [id: row["WorkflowOwner"] as Integer]
                if (row["InboxMetadata"]) {
                    config["variant-and-metadata-definition"] = [
                        variant: "###",
                        "metadata-definition": [id: row["InboxMetadata"] as Integer]
                    ]
                }
                
                configPayload = new JsonBuilder(config).toPrettyString()
                break
                
            default:
                println "⚠️ No configuration needed for type: ${type}"
                return
        }
        
        log("Setting configuration: ${configPayload}")
        
        def configConn = createConnection("${apiBaseUrl}/api/resources/${resourceId}/configuration", "PUT", configPayload)
        def (configRespCode, configRespText) = executeRequest(configConn)
        
        if (configRespCode < 300) {
            println "⚙️ Configured"
        } else {
            println "❌ Config failed (${configRespCode}): ${configRespText}"
        }
    }
    
    /**
     * Apply tags to a resource
     */
    void applyTags(Map<String, String> row, String resourceId) {
        def rawTags = row["Tags"]?.replaceAll('"', '')
        def tags = rawTags?.split(',')?.collect { it.trim().replaceAll('[\\r\\n]+', '') }?.findAll { it }
        
        if (!tags || tags.isEmpty()) return
        
        println "🏷️ Applying tags: ${tags.join(', ')}"
        
        def tagPayload = new JsonBuilder(tags).toPrettyString()
        def tagConn = createConnection("${apiBaseUrl}/api/resources/${resourceId}/tags", "POST", tagPayload)
        def (tagRespCode, tagRespText) = executeRequest(tagConn)
        
        if (tagRespCode < 300) {
            println "🏷️ Tags applied"
        } else {
            println "⚠️ Tagging failed (${tagRespCode}): ${tagRespText}"
        }
    }
    
    /**
     * Enable a resource
     */
    void enableResource(String resourceId) {
        def enablePayload = new JsonBuilder([action: "enable", options: [:]]).toPrettyString()
        def enableConn = createConnection("${apiBaseUrl}/api/resources/${resourceId}/actions", "POST", enablePayload)
        def (enableRespCode, enableRespText) = executeRequest(enableConn)
        
        if (enableRespCode < 300) {
            println "🟢 Enabled"
        } else {
            println "⚠️ Enable failed (${enableRespCode}): ${enableRespText}"
        }
    }
    
    /**
     * Create an HTTP connection with appropriate headers
     */
    HttpURLConnection createConnection(String url, String method, String payload = null) {
        def conn = (HttpURLConnection) new URL(url).openConnection()
        conn.with {
            requestMethod = method
            setRequestProperty('Content-Type', 'application/vnd.nativ.mio.v1+json')
            setRequestProperty('Authorization', "Basic ${basicAuth}")
            
            if (payload) {
                doOutput = true
                outputStream.withWriter("UTF-8") { it << payload }
            }
        }
        
        return conn
    }
    
    /**
     * Execute an HTTP request and return response code and text
     */
    def executeRequest(HttpURLConnection conn) {
        def respCode = conn.responseCode
        def respText = respCode < 300 ? conn.inputStream.text : conn.errorStream.text
        
        log("Response [${respCode}]: ${respText}")
        
        return [respCode, respText]
    }
    
    /**
     * Log a message if verbose mode is enabled
     */
    void log(String message) {
        if (verbose) {
            println "🔍 ${message}"
        }
    }
    
    /**
     * Check if a resource with the given name already exists
     */
    boolean resourceExists(String resourceName) {
        if (existingResourcesCache.containsKey(resourceName)) {
            return existingResourcesCache[resourceName]
        }
        
        try {
            def encodedName = URLEncoder.encode(resourceName, "UTF-8")
            def conn = createConnection("${apiBaseUrl}/api/resources;name=${encodedName}", "GET")
            def (respCode, respText) = executeRequest(conn)
            
            if (respCode == 200) {
                def response = jsonSlurper.parseText(respText)
                boolean exists = response.totalCount > 0
                existingResourcesCache[resourceName] = exists
                return exists
            } else {
                log("Failed to check if resource exists: ${respCode}")
                return false
            }
        } catch (Exception e) {
            log("Error checking if resource exists: ${e.message}")
            return false
        }
    }
    
    /**
     * Check if a workflow with the given ID exists
     */
    boolean workflowExists(String workflowId) {
        if (existingWorkflowCache.containsKey(workflowId)) {
            return existingWorkflowCache[workflowId]
        }
        
        try {
            def conn = createConnection("${apiBaseUrl}/api/workflowDefinitions/${workflowId}", "GET")
            def (respCode, respText) = executeRequest(conn)
            
            boolean exists = (respCode == 200)
            existingWorkflowCache[workflowId] = exists
            return exists
        } catch (Exception e) {
            log("Error checking if workflow exists: ${e.message}")
            return false
        }
    }
    
    /**
     * Check if a user with the given ID exists
     */
    boolean userExists(String userId) {
        if (existingUserCache.containsKey(userId)) {
            return existingUserCache[userId]
        }
        
        try {
            def conn = createConnection("${apiBaseUrl}/api/users/${userId}", "GET")
            def (respCode, respText) = executeRequest(conn)
            
            boolean exists = (respCode == 200)
            existingUserCache[userId] = exists
            return exists
        } catch (Exception e) {
            log("Error checking if user exists: ${e.message}")
            return false
        }
    }
    
    /**
     * Check if a metadata definition with the given ID exists
     */
    boolean metadataExists(String metadataId) {
        if (existingMetadataCache.containsKey(metadataId)) {
            return existingMetadataCache[metadataId]
        }
        
        try {
            def conn = createConnection("${apiBaseUrl}/api/metadataDefinitions/${metadataId}", "GET")
            def (respCode, respText) = executeRequest(conn)
            
            boolean exists = (respCode == 200)
            existingMetadataCache[metadataId] = exists
            return exists
        } catch (Exception e) {
            log("Error checking if metadata definition exists: ${e.message}")
            return false
        }
    }
    
    // Add missing boolean field
    boolean forceCreation = false
}

// Run the CLI
FlexPinotCli.main(args)