from github import Github
from tempfile import NamedTemporaryFile
from pyspark.sql.types import StructType

def generate_spark_join(dataframe1_name, dataframe2_name, dataframe1_file_path, dataframe2_file_path, join_column):
    schema1 = StructType() \
        .add("col1", "string") \
        .add("col2", "string") \
        .add("col3", "string") \
        .add("col4", "string") \
        .add("col5", "string")

    schema2 = StructType() \
        .add("col1", "string") \
        .add("col2", "string")

    spark_code = f"""
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("SparkJoinTransformation") \
    .getOrCreate()

# Read text files and create DataFrames
{dataframe1_name} = spark.read.option("header", "true").csv("{dataframe1_file_path}",schema={schema1})
{dataframe2_name} = spark.read.option("header", "true").csv("{dataframe2_file_path}",schema={schema2})

# Perform join transformation
{dataframe1_name}_joined = {dataframe1_name}.join({dataframe2_name}, on='{join_column}', how='inner')

# Show the resulting DataFrame
{dataframe1_name}_joined.show()

"""
    return spark_code

def create_github_release(repo_owner, repo_name, access_token, tag_name, release_name, release_body, asset_file_path):
    g = Github(access_token)
    repo = g.get_repo(f"{repo_owner}/{repo_name}")
    release = repo.create_git_release(tag=tag_name, name=release_name, message=release_body)
    release.upload_asset(asset_file_path)

def main():
    print("Welcome to the Spark Join Transformation Generator!")

    # Read text files and create DataFrames
    dataframe1_file_path = input("Enter the path to the first text file: ")
    dataframe1_name = input("Enter the name for the first DataFrame: ")

    dataframe2_file_path = input("Enter the path to the second text file: ")
    dataframe2_name = input("Enter the name for the second DataFrame: ")

    # Prompt user for join details
    join_column = input("Enter the column to perform the join on: ")
    output_dataframe_name = input("Enter the name for the output DataFrame: ")

    # Generate Spark code for join transformation
    spark_join_code = generate_spark_join(dataframe1_name, dataframe2_name, dataframe1_file_path, dataframe2_file_path, join_column)
    print("\nGenerated Spark Code:")
    print(spark_join_code)

    # Write Spark code to a temporary text file
    with NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_file.write(spark_join_code)
        asset_file_path = temp_file.name

    # Deploy the generated code to GitHub as an asset file
    deploy_to_github = input("Do you want to deploy the generated code to GitHub? (yes/no): ")
    if deploy_to_github.lower() == "yes":
        repo_owner = input("Enter the owner of the GitHub repository: ")
        repo_name = input("Enter the name of the GitHub repository: ")
        access_token = input("Enter your GitHub personal access token: ")
        tag_name = input("Enter the tag name for the release: ")
        release_name = input("Enter the name for the release: ")
        release_body = input("Enter the body for the release: ")

        create_github_release(repo_owner, repo_name, access_token, tag_name, release_name, release_body, asset_file_path)
        print("Release created successfully on GitHub!")
    else:
        print("Deployment to GitHub skipped.")

if __name__ == "__main__":
    main()
