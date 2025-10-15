FROM --platform=linux/amd64 ghcr.io/dbt-labs/dbt-snowflake:1.9.0

WORKDIR /dbt

# Copy dbt project files
COPY dbt/ .

# Ensure the profiles directory exists
RUN mkdir -p /root/.dbt
COPY dbt/profiles/ /root/.dbt/

# Install specific dbt version to avoid mismatch
RUN pip install --no-cache-dir --upgrade dbt-snowflake==1.9.0

# Check profiles folder
RUN echo "Contents of /root/.dbt:" && ls -la /root/.dbt

# Install dbt dependencies
RUN dbt deps
