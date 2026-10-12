import dlt

from pyspark.sql.functions import (
    array,
    col,
    coalesce,
    concat_ws,
    current_timestamp,
    explode,
    from_json,
    lit,
    max_by,
    split,
    struct,
    sum as spark_sum,
    to_date,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)


SCOPE = "confluent-keys"
TOPIC = spark.conf.get("nhl.kafka.topic", "events")

API_KEY = dbutils.secrets.get(scope=SCOPE, key="api-key")
API_SECRET = dbutils.secrets.get(scope=SCOPE, key="api-secret")
BOOTSTRAP_SERVER = dbutils.secrets.get(scope=SCOPE, key="bootstrap-server")

JAAS_CONFIG = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{API_KEY}" password="{API_SECRET}";'
)


ROSTER_SCHEMA = ArrayType(
    StructType([
        StructField("teamId", IntegerType(), True),
        StructField("playerId", LongType(), True),
        StructField("firstName", MapType(StringType(), StringType(), True), True),
        StructField("lastName", MapType(StringType(), StringType(), True), True),
        StructField("sweaterNumber", IntegerType(), True),
        StructField("positionCode", StringType(), True),
        StructField("headshot", StringType(), True),
    ])
)


EVENT_SCHEMA = StructType([
    StructField("game_id", IntegerType(), True),
    StructField("event_id", IntegerType(), True),
    StructField("period", IntegerType(), True),
    StructField("period_type", StringType(), True),
    StructField("time_in_period", StringType(), True),
    StructField("time_remaining", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("details", MapType(StringType(), StringType(), True), True),
    StructField("roster_spots", ROSTER_SCHEMA, True),
])


@dlt.table(
    name="nhl_events_bronze",
    comment="Raw NHL events consumed from Kafka.",
)
def nhl_events_bronze():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", JAAS_CONFIG)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("payload_json"),
            current_timestamp().alias("ingested_at"),
        )
    )


@dlt.table(
    name="nhl_events_silver",
    comment="Parsed and deduplicated NHL events.",
    partition_cols=["event_date"],
    table_properties={
        "pipelines.autoOptimize.managed": "true",
    },
)
def nhl_events_silver():
    return (
        dlt.read_stream("nhl_events_bronze")
        .filter(col("topic") == TOPIC)
        .withColumn("event", from_json(col("payload_json"), EVENT_SCHEMA))
        .select(
            col("kafka_key"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("ingested_at"),
            to_date(col("ingested_at")).alias("event_date"),
            col("event.*"),
        )
        .dropDuplicates(["partition", "offset"])
    )


@dlt.table(
    name="nhl_events_gold",
    comment="Player totals with names and current game state.",
    table_properties={
        "pipelines.autoOptimize.managed": "true",
    },
)
def nhl_events_gold():
    events = dlt.read("nhl_events_silver")

    events_with_clock = events.withColumn(
        "time_in_period_seconds",
        coalesce(
            split(col("time_in_period"), ":").getItem(0).cast("int") * 60
            + split(col("time_in_period"), ":").getItem(1).cast("int"),
            lit(-1),
        ),
    )

    roster_lookup = (
        events
        .filter(col("roster_spots").isNotNull())
        .select(explode("roster_spots").alias("roster"))
        .select(
            col("roster.playerId").cast("long").alias("player_id"),
            concat_ws(
                " ",
                col("roster.firstName").getItem("default"),
                col("roster.lastName").getItem("default"),
            ).alias("player_name"),
            col("roster.teamId").cast("int").alias("team_id"),
            col("roster.positionCode").alias("position"),
            col("roster.sweaterNumber").cast("int").alias("sweater_number"),
            col("roster.headshot").alias("headshot"),
        )
        .dropDuplicates(["player_id"])
    )

    latest_game_state = (
        events_with_clock
        .groupBy("game_id")
        .agg(
            max_by(
                struct(
                    col("period").alias("period"),
                    col("period_type").alias("period_type"),
                    col("time_in_period").alias("time_in_period"),
                    col("time_remaining").alias("time_remaining"),
                    col("event_id").alias("last_event_id"),
                    col("kafka_timestamp").alias("last_event_at"),
                ),
                struct(
                    coalesce(col("period"), lit(-1)),
                    col("time_in_period_seconds"),
                    coalesce(col("event_id"), lit(-1)),
                ),
            ).alias("latest_state")
        )
        .select(
            "game_id",
            col("latest_state.period").alias("period"),
            col("latest_state.period_type").alias("period_type"),
            col("latest_state.time_in_period").alias("time_in_period"),
            col("latest_state.time_remaining").alias("time_remaining"),
            col("latest_state.last_event_id").alias("last_event_id"),
            col("latest_state.last_event_at").alias("last_event_at"),
        )
    )

    goal_events = (
        events
        .filter(col("event_type") == "goal")
        .select(
            "game_id",
            "event_id",
            explode(
                array(
                    struct(
                        col("details").getItem("scoringPlayerId")
                        .cast("long").alias("player_id"),
                        lit(1).cast("long").alias("goals"),
                        lit(0).cast("long").alias("assists"),
                        lit(0).cast("long").alias("shots"),
                    ),
                    struct(
                        col("details").getItem("assist1PlayerId")
                        .cast("long").alias("player_id"),
                        lit(0).cast("long").alias("goals"),
                        lit(1).cast("long").alias("assists"),
                        lit(0).cast("long").alias("shots"),
                    ),
                    struct(
                        col("details").getItem("assist2PlayerId")
                        .cast("long").alias("player_id"),
                        lit(0).cast("long").alias("goals"),
                        lit(1).cast("long").alias("assists"),
                        lit(0).cast("long").alias("shots"),
                    ),
                )
            ).alias("contribution"),
        )
        .select(
            "game_id",
            "event_id",
            col("contribution.player_id").alias("player_id"),
            col("contribution.goals").alias("goals"),
            col("contribution.assists").alias("assists"),
            col("contribution.shots").alias("shots"),
        )
        .filter(col("player_id").isNotNull())
    )

    shot_events = (
        events
        .filter(col("event_type") == "shot-on-goal")
        .select(
            "game_id",
            "event_id",
            col("details").getItem("shootingPlayerId")
            .cast("long").alias("player_id"),
            lit(0).cast("long").alias("goals"),
            lit(0).cast("long").alias("assists"),
            lit(1).cast("long").alias("shots"),
        )
        .filter(col("player_id").isNotNull())
    )

    player_totals = (
        goal_events
        .unionByName(shot_events)
        .groupBy("game_id", "player_id")
        .agg(
            spark_sum("goals").alias("goals"),
            spark_sum("assists").alias("assists"),
            spark_sum("shots").alias("shots"),
        )
    )

    return (
        player_totals
        .join(roster_lookup, "player_id", "left")
        .join(latest_game_state, "game_id", "left")
        .withColumn("updated_at", current_timestamp())
        .select(
            "game_id",
            "player_id",
            "player_name",
            "position",
            "sweater_number",
            "headshot",
            "goals",
            "assists",
            "shots",
        )
    )
