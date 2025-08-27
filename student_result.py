import csv
import logging
#Configure logging
logging.basicConfig(
   level = logging.INFO,
   format = "%(asctime)a %(levelname)s %(message)s",
   filename = "ert.log",
   filemode="w"
)
def load_csv(file_path):
    try:
     with open(file_path,"r") as f:
      data=list(csv.DictReader(f))
      logging.info(f"Loaded {file_path} succesfully with {len(data)} records.")
      return data
    except Exception as e:
      logging.error(f"Error loading {file_path}: {e}")
      return []
def transform_data(records):
    for record in records:
      try:
       math_score = int(record["math_score"])
       reading_score = int(record["reading_score"])
       writing_score = int(record["writing_score"])
       avg_score =(math_score + reading_score + writing_score)/3
       calculate_grade = lambda  avg_score: "A" if avg_score > 80 else "B"
       record["grade"] = calculate_grade(avg_score)
       logging.debug(f"Transformed row:{record}")
      except Exception as e:
        print(e)
    return records
      

def save_csv(records,file_path):
   try:
      with open(file_path,"w",newline="") as f:
       writer=csv.writer(f)
       writer.writerow(["gender","race/ethnicity","parental_level_of_education","lunch","test_preparation_course","math_score","reading_score","writing_score","grade"]) #header
       for record in records:
         writer.writerow([record["gender"],record["race/ethnicity"],record["parental_level_of_education"],record["lunch"],record["test_preparation_course"],int(record["math_score"]),int(record["reading_score"]),int(record["writing_score"]),record["grade"]])
      logging.info(f"Failed to save {file_path}. :{e}")
   except Exception as e:
      logging.error(f"Failed to Save {file_path}: {e}")

#ETL Execution
data = load_csv("student.csv")
if data:
  transformed = transform_data(data)
  save_csv(transformed,"student_result.csv")
  logging.info("ETL pipeline completed successfully.")
else:
  logging.critical("ETL pipeline aborted - No data loaded.")