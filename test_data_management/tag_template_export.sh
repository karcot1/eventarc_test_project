#!/bin/bash

PROJECTID="" #modify this

echo "Exporting table and entry information..."
#Step 1: Find all objects that use the tag template in question, and have a value of "1" assigned to each respective level of data sensitivity. 
#Export all table and entry info to a CSV
gcloud data-catalog search 'tag:tdm_test_template.high:1' --include-project-ids=${PROJECTID} --format="csv(fullyQualifiedName,relativeResourceName)" > high.csv
gcloud data-catalog search 'tag:tdm_test_template.medium:1' --include-project-ids=${PROJECTID} --format="csv(fullyQualifiedName,relativeResourceName)" > medium.csv
gcloud data-catalog search 'tag:tdm_test_template.low:1' --include-project-ids=${PROJECTID} --format="csv(fullyQualifiedName,relativeResourceName)" > low.csv

#loop through each table and export policy tags (if any) to a CSV
echo "Writing to CSV..."

#export high sensitivity columns
{
    echo "TABLE,COLUMN,SENSITIVITY"
    read
    while IFS=, read -r fqn rrn
    do
        TABLE="${fqn//bigquery:}"
        COLUMN=`gcloud data-catalog tags list --entry=$rrn --format=json | jq '.[] | .column'`
        echo ${TABLE},${COLUMN//},"HIGH" | tr -d '"'
    done
} < high.csv >> tag_templates_export.csv

#export medium sensitivity columns
{
    read
    while IFS=, read -r fqn rrn
    do
        TABLE="${fqn//bigquery:}"
        COLUMN=`gcloud data-catalog tags list --entry=$rrn --format=json | jq '.[] | .column'`
        echo ${TABLE},${COLUMN//},"MEDIUM" | tr -d '"'
    done
} < medium.csv >> tag_templates_export.csv

#export low sensitivity columns
{
    read
    while IFS=, read -r fqn rrn
    do
        TABLE="${fqn//bigquery:}"
        COLUMN=`gcloud data-catalog tags list --entry=$rrn --format=json | jq '.[] | .column'`
        echo ${TABLE},${COLUMN//},"LOW" | tr -d '"'
    done
} < low.csv >> tag_templates_export.csv

echo "Done."