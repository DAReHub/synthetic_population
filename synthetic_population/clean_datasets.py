import glob
import pandas as pd
import os
import time
from dotenv import load_dotenv

load_dotenv()
PERSONS_INPUT_DIR = os.getenv("clean_datasets_test_persons_dir")
HOUSEHOLDS_INPUT_DIR = os.getenv("clean_datasets_test_households_dir")
MSOA_OA_INPUT_PATH = os.getenv("clean_datasets_test_MSOA_OA_path")
PERSONS_OUTPUT_PATH = os.getenv("")
HOUSEHOLDS_OUTPUT_PATH = os.getenv("")

def load_df(path):
    print("loading csv to dataframe: ", path)
    files = glob.iglob(os.path.join(path, "*.csv"))
    temp = []
    for filename in files:
        df = pd.read_csv(filename, index_col=None, header=0)
        temp.append(df)
    return pd.concat(temp, axis=0, ignore_index=True)


def remove_outliers(df):
    residential_type_list = [-2, 26]
    if 'QS420_CELL' in df.columns:
        return df.loc[df['QS420_CELL'].isin(residential_type_list)]
    elif 'QS420_CELL_x' in df.columns:
        return df.loc[df['QS420_CELL_x'].isin(residential_type_list)]


def export_to_csv(df, path):
    print("exporting to csv: ", path)
    df.to_csv(path, encoding='utf-8', header=True)


def main():
    start_time = time.time()

    df_persons_NE = load_df(PERSONS_INPUT_DIR)
    df_households_NE = load_df(HOUSEHOLDS_INPUT_DIR)
    df_MSOA_OA_2011_NE_list = pd.read_csv(MSOA_OA_INPUT_PATH, index_col=None,
                                          header=0)

    print("No.of.unique person values :", len(pd.unique(df_persons_NE['Area'])))
    print("No.of.unique household values :",
          len(pd.unique(df_households_NE['Area'])))

    # Modify columns
    df_persons_NE = df_persons_NE.rename(
        {'Area': 'Area_MSOA', 'DC1117EW_C_SEX': 'Sex',
         'DC1117EW_C_AGE': 'Age', 'DC2101EW_C_ETHPUK11': 'Ethnic'}, axis=1)
    df_households_NE = df_households_NE.rename({'Area': 'Area_OA'}, axis=1)
    df_households_NE = df_households_NE.astype({"HID": int})

    # Create a new column in 'df_persons_NE' for the unique ID --> PID_Area_MSOA
    df_persons_NE["PID_AreaMSOA"] = df_persons_NE["PID"].astype(str) + '_' + \
                                    df_persons_NE["Area_MSOA"]
    df_households_NE["HID_AreaOA"] = df_households_NE["HID"].astype(str) + '_' \
                                     + df_households_NE["Area_OA"]

    # Join df_households_NE with df_MSOA_OA_2011_NE_list in order to get MSOA
    # level within df_households_NE and then pass the Area_OA to df_persons_NE
    # based on the MSOA level value
    df_households_NE = df_households_NE.merge(df_MSOA_OA_2011_NE_list,
                                              left_on='Area_OA', right_on='oa',
                                              how='left')

    # Rename the column names of 'df_persons_NE'
    df_households_NE = df_households_NE.rename({'msoa': 'Area_MSOA'}, axis=1)

    # Merge df_persons_NE with df_households_NE in order to get the Area_OA for
    # each person
    df_persons_NE = pd.merge(df_persons_NE, df_households_NE, how='left',
                             left_on=['HID', 'Area_MSOA'],
                             right_on=['HID', 'Area_MSOA'])

    # Create a new column in df_households_NE combining HRPID with Area_MSOA in
    # order to then match the Household refernce person (HRPID) from the
    # household dataframe with the person in the df_persons_NE
    df_households_NE["HRPID_AreaMSOA"] = df_households_NE["HRPID"].astype(
        str) + '_' + df_households_NE["Area_MSOA"]

    # Merge df_persons_NE with df_households_NE AGAIN in order to get the
    # HRPID_Area_MSOA for each person. The goal with this is that the new
    # LC4605_C_NSSEC will be only assigned to the household reference person.
    # The rest of member of the household will have an empty value in this
    # column
    df_persons_NE = pd.merge(df_persons_NE, df_households_NE, how='left',
                             left_on=['PID_AreaMSOA'],
                             right_on=['HRPID_AreaMSOA'])

    # KEEP ONLY the required columns and rename
    df_persons_NE = df_persons_NE[
        ['PID_AreaMSOA', 'PID', 'Area_MSOA_x', 'Area_OA_x', 'Sex', 'Age',
         'Ethnic', 'HID_x', 'HID_AreaOA_x', 'LC4408_C_AHTHUK11_x',
         'LC4404_C_SIZHUK11_x', 'LC4605_C_NSSEC_x', 'LC4202_C_CARSNO_x',
         'LC4202_C_ETHHUK11_x', 'HRPID_AreaMSOA', 'LC4605_C_NSSEC_y',
         'QS420_CELL_x']]
    df_persons_NE = df_persons_NE.rename({'LC4605_C_NSSEC_y': 'NSSEC'}, axis=1)

    # Remove rows from df_persons_NE which HIP = (-1), as they are not assigned
    # to any household. They should be considered as leftovers in the process of
    # matching persons with households
    # TODO: Changed 'HID' to 'HID_x' as there is no 'HID' column after selecting
    # TODO: required columns in persons. Is this correct?
    df_persons_NE = df_persons_NE[df_persons_NE['HID_x'] != -1]

    # Remove those PEOPLE and HOUSEHOLDS that do not belong to residential
    # households (QS420_CELL = -2) or to student accommodations
    # (QS420_CELL = 26)
    df_persons_NE = remove_outliers(df_persons_NE)
    df_households_NE_extended = remove_outliers(df_households_NE)

    # Export dataframes into csv files
    export_to_csv(df_persons_NE, PERSONS_OUTPUT_PATH)
    # TODO: Should this df be 'df_households_NE' or 'df_households_NE_extended'?
    export_to_csv(df_households_NE_extended, HOUSEHOLDS_OUTPUT_PATH)

    end_time = time.time()
    print(f"Cleaned datasets - completed in {round(end_time - start_time)} "
          f"seconds")


if __name__ == "__main__":
    main()
