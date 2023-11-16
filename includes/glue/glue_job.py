from pyspark.sql.functions import lit
import sys
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, explode, lit, concat
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job


# sc = SparkContext()
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

## fixtures
glue_df = glueContext.create_dynamic_frame.from_catalog(
            database="football_db",
            table_name="fixtures"
        )
fixtures_df = glue_df.toDF()
fixtures_response_df = fixtures_df.select("response")
# Explode the array to create a new row for each JSON object in the array
fixtures_exploded_df = fixtures_response_df.select(explode("response").alias("fixture"))
fixtures_final_df = fixtures_exploded_df.select(
    col("fixture.fixture.id").alias("fixture_id"),
    col("fixture.fixture.referee").alias("referee"),
    col("fixture.fixture.timezone").alias("timezone"),
    col("fixture.fixture.date").alias("date"),
    col("fixture.fixture.venue.name").alias("Stadium"),
    col("fixture.teams.home.name").alias("Home team"),
    col("fixture.teams.away.name").alias("Away team"),
)
fixtures_dynamic_frame = DynamicFrame.fromDF(fixtures_final_df, glueContext, "fixtures_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://footballapi-data-bucket/processed/fixtures"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = fixtures_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
##results
glue_df = glueContext.create_dynamic_frame.from_catalog(
            database="football_db",
            table_name="results"
        )
results_df = glue_df.toDF()
results_response_df = results_df.select("response")
results_exploded_df = results_response_df.select(explode("response").alias("results"))
results_final_df = results_exploded_df.select(
    col("results.fixture.id").alias("fixture_id"),
    col("results.fixture.referee").alias("referee"),
    col("results.fixture.timezone").alias("timezone"),
    col("results.fixture.date").alias("date"),
    col("results.fixture.venue.name").alias("Stadium"),
    col("results.teams.home.name").alias("Home team"),
    col("results.teams.away.name").alias("Away team"),
    col("results.goals.home").alias("Home goals"),
    col("results.goals.away").alias("Away goals"),
    col("results.score.halftime.Home").alias("Half time Home goals"),
    col("results.score.halftime.away").alias("Half time Away goals"),
)
columns_to_drop = ["Home goals","Away goals", "Half time Home goals", "Half time Away goals"]
results_final_df = results_final_df.withColumn("Full time score", concat(col("Home goals"), lit(":"), col("Away Goals")))
results_final_df = results_final_df.withColumn("Half time score", concat(col("Half time Home goals"), lit(":"), col("Half time Away goals")))
results_final_df = results_final_df.drop(*columns_to_drop)
results_dynamic_frame = DynamicFrame.fromDF(results_final_df, glueContext, "results_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://footballapi-data-bucket/processed/results"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = results_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)

##standings
glue_df = glueContext.create_dynamic_frame.from_catalog(
            database="football_db",
            table_name="standings"
        )
standings_df = glue_df.toDF()
standings_response_df = standings_df.select("response")
# Explode the array to create a new row for each JSON object in the array
standings_exploded_df = standings_response_df.select(explode("response.league.standings").alias("standings"))
standings_exploded_df.show(truncate=True)
standings_exploded_df = standings_response_df.select(
    explode("response.league.standings").alias("league"),
).select(
    explode("league").alias("standings")
).select(
    explode("standings").alias("standings")
)
standings_final_df = standings_exploded_df.select(
    col("standings.rank").alias("League Position"),
    col("standings.team.name").alias("Team"),
    col("standings.points").alias("League Points"),
    col("standings.all.win").alias("Wins"),
    col("standings.all.draw").alias("Draws"),
    col("standings.all.lose").alias("Loses"),
    col("standings.goalsDiff").alias("Goal Difference"),
    col("standings.all.goals.for").alias("Goals For"),
    col("standings.all.goals.against").alias("Goals Against"),
    col("standings.form").alias("Form"),
    col("standings.away.win").alias("Away wins"),
    col("standings.home.lose").alias("Home loses"),    
)
standings_dynamic_frame = DynamicFrame.fromDF(standings_final_df, glueContext, "standings_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://footballapi-data-bucket/processed/standings"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = standings_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
##goal scorers
glue_df = glueContext.create_dynamic_frame.from_catalog(
            database="football_db",
            table_name="scorers"
        )
scorers_df = glue_df.toDF()
scorers_response_df = scorers_df.select("response")
# Explode the array to create a new row for each JSON object in the array
scorers_exploded_df = scorers_response_df.select(explode("response").alias("topscorers"))
scorers_final_df = scorers_exploded_df.select(
    col("topscorers.player.name").alias("Name"),
    col("topscorers.statistics.goals.total").alias("Goals scored"),
    col("topscorers.statistics.shots.on").alias("Shots on Target"),
    col("topscorers.statistics.shots.total").alias("Shots"),
    col("topscorers.statistics.penalty.scored").alias("Penalties scored"),
)

scorers_final_df = scorers_final_df.withColumn("Shots on Target", col("Shots on Target").getItem(0))
scorers_final_df = scorers_final_df.withColumn("Goals scored", col("Goals scored").getItem(0))
scorers_final_df = scorers_final_df.withColumn("Shots", col("Shots").getItem(0))
scorers_final_df = scorers_final_df.withColumn("Penalties scored", col("Penalties scored").getItem(0))

scorers_dynamic_frame = DynamicFrame.fromDF(scorers_final_df, glueContext, "scorers_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://footballapi-data-bucket/processed/scorers"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = scorers_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
##assists
glue_df = glueContext.create_dynamic_frame.from_catalog(
            database="football_db",
            table_name="assists"
        )
assists_df = glue_df.toDF()
assists_response_df = assists_df.select("response")
# Explode the array to create a new row for each JSON object in the array
assists_exploded_df = assists_response_df.select(explode("response").alias("assists"))
assists_final_df = assists_exploded_df.select(
    col("assists.player.name").alias("Name"),
    col("assists.statistics.goals.assists").alias("Assists"),
    col("assists.statistics.passes.key").alias("Key Passes"),
    col("assists.statistics.shots.on").alias("Shots on Target"),
    col("assists.statistics.passes.total").alias("Total successfull passes"),
)
assists_final_df = assists_final_df.withColumn("Assists", col("Assists").getItem(0))
assists_final_df = assists_final_df.withColumn("Key Passes", col("Key Passes").getItem(0))
assists_final_df = assists_final_df.withColumn("Shots on Target", col("Shots on Target").getItem(0))
assists_final_df = assists_final_df.withColumn("Total successfull passes", col("Total successfull passes").getItem(0))

assists_dynamic_frame = DynamicFrame.fromDF(assists_final_df, glueContext, "assists_dynamic_frame")

# Specify the S3 output path where you want to store the CSV file
output_path = "s3://footballapi-data-bucket/processed/assists"

# Write the DynamicFrame to S3 as a CSV file
glueContext.write_dynamic_frame.from_options(
    frame = assists_dynamic_frame,
    connection_type = "s3",
    connection_options = {"path": output_path},
    format = "csv",
    format_options = {"writeHeader": True},
)
job.commit()
