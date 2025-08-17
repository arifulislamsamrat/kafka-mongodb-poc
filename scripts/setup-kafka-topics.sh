#!/bin/bash

# Kafka Topics Setup Script
# This script creates all necessary Kafka topics for the POC

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
KAFKA_BROKERS=${KAFKA_BROKERS:-"localhost:9092"}
REPLICATION_FACTOR=${REPLICATION_FACTOR:-1}
PARTITIONS=${PARTITIONS:-3}
RETENTION_MS=${RETENTION_MS:-604800000}  # 7 days in milliseconds
CLEANUP_POLICY=${CLEANUP_POLICY:-"delete"}
MIN_INSYNC_REPLICAS=${MIN_INSYNC_REPLICAS:-1}

# Topic configurations
declare -A TOPICS=(
    ["user-events"]="Main topic for user events"
    ["user-events-dlq"]="Dead letter queue for failed messages"
    ["user-events-retry"]="Retry topic for failed messages"
    ["system-events"]="System and application events"
    ["audit-log"]="Audit log for data pipeline"
)

# Special topic configurations (override defaults)
declare -A TOPIC_PARTITIONS=(
    ["user-events"]=6
    ["user-events-dlq"]=3
    ["user-events-retry"]=3
    ["system-events"]=3
    ["audit-log"]=1
)

declare -A TOPIC_RETENTION=(
    ["user-events"]=2592000000      # 30 days
    ["user-events-dlq"]=1209600000  # 14 days
    ["user-events-retry"]=86400000  # 1 day
    ["system-events"]=604800000     # 7 days
    ["audit-log"]=7776000000        # 90 days
)

declare -A TOPIC_CLEANUP=(
    ["user-events"]="delete"
    ["user-events-dlq"]="delete"
    ["user-events-retry"]="delete"
    ["system-events"]="delete"
    ["audit-log"]="compact,delete"
)

# Functions
print_header() {
    echo -e "${BLUE}=================================================${NC}"
    echo -e "${BLUE}          Kafka Topics Setup Script${NC}"
    echo -e "${BLUE}=================================================${NC}"
    echo ""
}

print_config() {
    echo -e "${YELLOW}Configuration:${NC}"
    echo "  Kafka Brokers: $KAFKA_BROKERS"
    echo "  Default Replication Factor: $REPLICATION_FACTOR"
    echo "  Default Partitions: $PARTITIONS"
    echo "  Default Retention: $RETENTION_MS ms"
    echo "  Min In-Sync Replicas: $MIN_INSYNC_REPLICAS"
    echo ""
}

check_kafka_availability() {
    echo -e "${YELLOW}Checking Kafka availability...${NC}"
    
    if command -v kafka-topics.sh &> /dev/null; then
        KAFKA_TOPICS_CMD="kafka-topics.sh"
    elif command -v kafka-topics &> /dev/null; then
        KAFKA_TOPICS_CMD="kafka-topics"
    elif [ -f "/opt/kafka/bin/kafka-topics.sh" ]; then
        KAFKA_TOPICS_CMD="/opt/kafka/bin/kafka-topics.sh"
    elif [ -f "/usr/local/bin/kafka-topics" ]; then
        KAFKA_TOPICS_CMD="/usr/local/bin/kafka-topics"
    else
        echo -e "${RED}Error: kafka-topics command not found!${NC}"
        echo "Please ensure Kafka is installed and in your PATH"
        echo "Or run this script from the Kafka installation directory"
        exit 1
    fi
    
    # Test connection to Kafka
    if ! $KAFKA_TOPICS_CMD --bootstrap-server $KAFKA_BROKERS --list &> /dev/null; then
        echo -e "${RED}Error: Cannot connect to Kafka brokers at $KAFKA_BROKERS${NC}"
        echo "Please ensure Kafka is running and accessible"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Kafka is available and accessible${NC}"
    echo "  Using command: $KAFKA_TOPICS_CMD"
    echo ""
}

list_existing_topics() {
    echo -e "${YELLOW}Existing topics:${NC}"
    EXISTING_TOPICS=$($KAFKA_TOPICS_CMD --bootstrap-server $KAFKA_BROKERS --list 2>/dev/null || echo "")
    
    if [ -z "$EXISTING_TOPICS" ]; then
        echo "  No topics found"
    else
        echo "$EXISTING_TOPICS" | sed 's/^/  /'
    fi
    echo ""
}

topic_exists() {
    local topic=$1
    $KAFKA_TOPICS_CMD --bootstrap-server $KAFKA_BROKERS --list 2>/dev/null | grep -q "^${topic}$"
}

create_topic() {
    local topic=$1
    local description=$2
    local partitions=${TOPIC_PARTITIONS[$topic]:-$PARTITIONS}
    local retention=${TOPIC_RETENTION[$topic]:-$RETENTION_MS}
    local cleanup=${TOPIC_CLEANUP[$topic]:-$CLEANUP_POLICY}
    
    echo -e "${YELLOW}Creating topic: $topic${NC}"
    echo "  Description: $description"
    echo "  Partitions: $partitions"
    echo "  Replication Factor: $REPLICATION_FACTOR"
    echo "  Retention: $retention ms"
    echo "  Cleanup Policy: $cleanup"
    
    if topic_exists "$topic"; then
        echo -e "${YELLOW}  ⚠ Topic already exists, skipping creation${NC}"
        return 0
    fi
    
    # Build the create command
    local create_cmd="$KAFKA_TOPICS_CMD --create \
        --bootstrap-server $KAFKA_BROKERS \
        --topic $topic \
        --partitions $partitions \
        --replication-factor $REPLICATION_FACTOR \
        --config retention.ms=$retention \
        --config cleanup.policy=$cleanup \
        --config min.insync.replicas=$MIN_INSYNC_REPLICAS"
    
    # Add compression for high-throughput topics
    if [[ "$topic" == "user-events" ]]; then
        create_cmd="$create_cmd --config compression.type=gzip"
    fi
    
    # Add compaction settings for audit log
    if [[ "$topic" == "audit-log" ]]; then
        create_cmd="$create_cmd \
            --config segment.ms=86400000 \
            --config delete.retention.ms=86400000 \
            --config min.compaction.lag.ms=3600000"
    fi
    
    if eval $create_cmd; then
        echo -e "${GREEN}  ✓ Topic created successfully${NC}"
    else
        echo -e "${RED}  ✗ Failed to create topic${NC}"
        return 1
    fi
    echo ""
}

verify_topic() {
    local topic=$1
    
    echo -e "${YELLOW}Verifying topic: $topic${NC}"
    
    if ! topic_exists "$topic"; then
        echo -e "${RED}  ✗ Topic does not exist${NC}"
        return 1
    fi
    
    # Get topic details
    local details=$($KAFKA_TOPICS_CMD --bootstrap-server $KAFKA_BROKERS --describe --topic $topic 2>/dev/null)
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}  ✓ Topic verified${NC}"
        echo "$details" | grep -E "(Topic:|PartitionCount:|ReplicationFactor:)" | sed 's/^/    /'
    else
        echo -e "${RED}  ✗ Failed to verify topic${NC}"
        return 1
    fi
    echo ""
}

create_all_topics() {
    echo -e "${YELLOW}Creating topics...${NC}"
    echo ""
    
    local failed_topics=()
    
    for topic in "${!TOPICS[@]}"; do
        if ! create_topic "$topic" "${TOPICS[$topic]}"; then
            failed_topics+=("$topic")
        fi
    done
    
    if [ ${#failed_topics[@]} -gt 0 ]; then
        echo -e "${RED}Failed to create topics: ${failed_topics[*]}${NC}"
        return 1
    fi
    
    echo -e "${GREEN}All topics created successfully!${NC}"
    echo ""
}

verify_all_topics() {
    echo -e "${YELLOW}Verifying all topics...${NC}"
    echo ""
    
    local failed_verifications=()
    
    for topic in "${!TOPICS[@]}"; do
        if ! verify_topic "$topic"; then
            failed_verifications+=("$topic")
        fi
    done
    
    if [ ${#failed_verifications[@]} -gt 0 ]; then
        echo -e "${RED}Failed to verify topics: ${failed_verifications[*]}${NC}"
        return 1
    fi
    
    echo -e "${GREEN}All topics verified successfully!${NC}"
    echo ""
}

setup_consumer_groups() {
    echo -e "${YELLOW}Setting up consumer groups...${NC}"
    echo ""
    
    # Consumer groups will be created automatically when consumers connect
    # But we can pre-create them with specific configurations if needed
    
    local consumer_groups=(
        "mongodb-consumer-group"
        "analytics-consumer-group"
        "audit-consumer-group"
        "dlq-processor-group"
    )
    
    for group in "${consumer_groups[@]}"; do
        echo "  Consumer group: $group (will be created on first consumer connection)"
    done
    echo ""
}

cleanup_old_topics() {
    echo -e "${YELLOW}Checking for old/test topics to cleanup...${NC}"
    
    # List of patterns for topics that might need cleanup
    local cleanup_patterns=(
        "test-*"
        "*-temp"
        "*-debug"
        "tmp-*"
    )
    
    local topics_to_delete=()
    
    for pattern in "${cleanup_patterns[@]}"; do
        local matching_topics=$($KAFKA_TOPICS_CMD --bootstrap-server $KAFKA_BROKERS --list 2>/dev/null | grep -E "^${pattern//\*/.*}$" || true)
        if [ ! -z "$matching_topics" ]; then
            topics_to_delete+=($matching_topics)
        fi
    done
    
    if [ ${#topics_to_delete[@]} -gt 0 ]; then
        echo "  Found topics that might need cleanup:"
        printf '    %s\n' "${topics_to_delete[@]}"
        echo ""
        echo "  To delete these topics manually, run:"
        for topic in "${topics_to_delete[@]}"; do
            echo "    $KAFKA_TOPICS_CMD --bootstrap-server $KAFKA_BROKERS --delete --topic $topic"
        done
    else
        echo "  No cleanup needed"
    fi
    echo ""
}

print_final_summary() {
    echo -e "${BLUE}=================================================${NC}"
    echo -e "${BLUE}                  Summary${NC}"
    echo -e "${BLUE}=================================================${NC}"
    echo ""
    
    echo -e "${GREEN}✓ Topics setup completed successfully${NC}"
    echo ""
    
    echo "Created topics:"
    for topic in "${!TOPICS[@]}"; do
        echo "  • $topic - ${TOPICS[$topic]}"
    done
    echo ""
    
    echo "Next steps:"
    echo "  1. Start your Kafka consumer:"
    echo "     npm run start:consumer"
    echo ""
    echo "  2. Start the test producer:"
    echo "     npm run start:producer"
    echo ""
    echo "  3. Monitor topics:"
    echo "     $KAFKA_TOPICS_CMD --bootstrap-server $KAFKA_BROKERS --list"
    echo ""
    echo "  4. Check consumer groups:"
    echo "     kafka-consumer-groups.sh --bootstrap-server $KAFKA_BROKERS --list"
    echo ""
    
    echo -e "${BLUE}=================================================${NC}"
}

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Setup Kafka topics for the Kafka-MongoDB POC"
    echo ""
    echo "Options:"
    echo "  -b, --brokers BROKERS     Kafka bootstrap servers (default: localhost:9092)"
    echo "  -r, --replication FACTOR  Replication factor (default: 1)"
    echo "  -p, --partitions COUNT    Default partition count (default: 3)"
    echo "  -t, --retention MS        Default retention in milliseconds (default: 604800000)"
    echo "  --cleanup                 Show cleanup commands for old topics"
    echo "  --verify-only             Only verify existing topics, don't create new ones"
    echo "  --list-only               Only list existing topics"
    echo "  -h, --help                Show this help"
    echo ""
    echo "Environment variables:"
    echo "  KAFKA_BROKERS            Kafka bootstrap servers"
    echo "  REPLICATION_FACTOR       Topic replication factor"
    echo "  PARTITIONS               Default partition count"
    echo "  RETENTION_MS             Default retention in milliseconds"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Use defaults"
    echo "  $0 -b kafka1:9092,kafka2:9092        # Multiple brokers"
    echo "  $0 -r 3 -p 6                         # 3 replicas, 6 partitions"
    echo "  $0 --verify-only                     # Just verify existing topics"
    echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--brokers)
            KAFKA_BROKERS="$2"
            shift 2
            ;;
        -r|--replication)
            REPLICATION_FACTOR="$2"
            shift 2
            ;;
        -p|--partitions)
            PARTITIONS="$2"
            shift 2
            ;;
        -t|--retention)
            RETENTION_MS="$2"
            shift 2
            ;;
        --cleanup)
            print_header
            check_kafka_availability
            cleanup_old_topics
            exit 0
            ;;
        --verify-only)
            print_header
            check_kafka_availability
            verify_all_topics
            exit 0
            ;;
        --list-only)
            print_header
            check_kafka_availability
            list_existing_topics
            exit 0
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Main execution
main() {
    print_header
    print_config
    check_kafka_availability
    list_existing_topics
    create_all_topics
    verify_all_topics
    setup_consumer_groups
    cleanup_old_topics
    print_final_summary
}

# Run main function
main