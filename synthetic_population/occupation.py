# Assigns occupation codes to the working population
# ! Running this can be memory intensive !

import pandas as pd
import numpy as np
import os
import json
import time
from dotenv import load_dotenv

load_dotenv()
HOUSEHOLDS_PATH = os.getenv("occupation_test_households_path")
EMPLOYED_PATH = os.getenv("occupation_test_employed_path")
UNEMPLOYED_PATH = os.getenv("occupation_test_unemployed_path")
CENSUS_OCCUPATION_PATH = os.getenv("occupation_test_census_occupation_path")
OCCUPATION_PATH = os.getenv("")

with open("config.json") as f:
    config = json.load(f)
GENDERS = config["genders"]
AGE_RANGES = config["age_ranges"]
OCCUPATION_CODES = config["occupation"]["occupation_codes"]
OCCUPATIONS = config["occupation"]["gender_occupations"]
OCCUPATIONS_LAST = config["occupation"]["gender_occupations_last"]
CONVERSORS = config["occupation"]["conversors"]
PERCENTAGES = config["occupation"]["gender_percentages"]


def get_occupation_dataframe(df_employed, df_unemployed):
    # Concatenate employed and unemployed people, and create new column
    df = pd.concat([df_employed, df_unemployed])
    df['Occupation'] = np.nan
    # Get only those that are >=16 years old only
    return df.loc[(df['Age'] >= 16)]


def get_AreaOA_list(df_households):
    # LIST OF OA_AREAS that have been generated before
    # Create a list with all Households unique ID values and remove duplicates
    AreaOA_list = df_households['Area_OA'].tolist()
    return list(set(AreaOA_list))


# TODO: Function also used in economic_activity.py - abstract?
def sample_dataframe(df, value):
    df_len = len(df.index)
    if df_len == 0:
        return pd.DataFrame()
    # If df is less than value, all of the df is applied as a sample
    return df.sample(min(value, df_len))


# TODO: Make census data labels generic
def calculate_ratio(total_2011, total_2019):
    if total_2011 > 0:
        return total_2019 / total_2011
    return 1


def get_values_2011(gender, age_range, df):
    values = []
    for occupation in OCCUPATIONS[gender]:
        column = f"occupation_{occupation}_{age_range[0]}_{age_range[1]}"
        values.append(df.iloc[0, df.columns.get_loc(column)])
    return values


def first_stage_process_occupation(gender, df_genders, occupation, values_2011,
                                   ratio, occupation_OAareas_2019):

    # Note - use occupation value as index
    # TODO: When using occupation value for indexing, I have stepped it down by
    #  one as indexing was previously starting at 1 rather than 0 (I think?).
    #  Are other parts of the code affected by this situation?
    conversor = CONVERSORS[occupation - 1]
    percentage = PERCENTAGES[gender][occupation - 1]
    value_2011 = values_2011[occupation - 1]

    # Update the value to 2019: 2019 = 2011 * occupation_"X"_conversor *
    # ratio_people_2019_2011
    occupation_OAarea_2019 = int(round(value_2011 * conversor * ratio, 0))
    occupation_OAareas_2019[occupation] = int(round(
        occupation_OAarea_2019 * percentage / 100, 0))

    # Select people randomly by age range and sex
    df_selected = sample_dataframe(df_genders[gender], occupation_OAarea_2019)

    # Update the column value to the occupation selected:
    df_selected["Occupation"] = occupation

    # Concatenate the selected ones with the people from the same age and OA
    # area
    df_plus = pd.concat([df_genders[gender], df_selected])

    # Remove duplicates BUT keep the same names of the dataframes used after
    # selecting the OAarea, age and sex
    df_genders[gender] = df_plus.drop_duplicates(subset='PID_AreaMSOA',
                                                 keep=False)

    return df_selected, df_genders, occupation_OAareas_2019


def second_stage_process_occupation(gender, df_genders, occupation, age_range,
                                    OA_area, first_selection,
                                    occupation_OAareas_2019):

    df_selection_male = first_selection[
        f"{OA_area}_{age_range[0]}-{age_range[1]}_male_{occupation}"]
    df_selection_female = first_selection[
        f"{OA_area}_{age_range[0]}-{age_range[1]}_female_{occupation}"]

    occupation_OAarea_2019 = occupation_OAareas_2019[occupation]

    remaining = (occupation_OAarea_2019 - len(df_selection_male.index)
                 - len(df_selection_female.index))

    # TODO: test refactored logic
    if 0 < remaining < len(df_genders[gender]):
        df_selected = df_genders[gender].sample(remaining)
    elif remaining >= len(df_genders[gender]):
        df_selected = df_genders[gender]
    else:
        df_selected = pd.DataFrame()

    # Update the column value to the occupation selected:
    df_selected["Occupation"] = occupation

    # Concatenate the selected ones with the people from the same
    # age and OA area
    df_plus = pd.concat([df_genders[gender], df_selected])

    # Remove duplicates BUT keep the same names of the dataframes
    # used after selecting the OAarea, age and sex
    df_genders[gender] = df_plus.drop_duplicates(subset='PID_AreaMSOA',
                                                 keep=False)

    return df_selected, df_genders


def first_stage(df_genders, age_range, OA_area, df_occupation_OAarea,
                total_2019):
    selections = {}
    occupation_OAareas_2019 = {}

    for gender in GENDERS:
        # Calculate the total number of people in the range of age that are
        # found in the census data and ratio of people 2019 vs 2011
        values_2011 = get_values_2011(gender, age_range, df_occupation_OAarea)
        total_2011 = sum(values_2011)
        ratio = calculate_ratio(total_2011, total_2019)

        for occupation in OCCUPATIONS[gender]:
            key = (f"{OA_area}_{age_range[0]}-{age_range[1]}_{gender}_"
                   f"{occupation}")

            selections[key], df_genders, occupation_OAareas_2019 = (
                first_stage_process_occupation(
                    gender, df_genders, occupation, values_2011, ratio,
                    occupation_OAareas_2019
                )
            )

    return selections, df_genders, occupation_OAareas_2019


def second_stage(df_genders, age_range, OA_area, first_selection,
                 occupation_OAareas_2019):
    selections = []

    for gender in GENDERS:
        # TODO: Assumed use of len() here - check
        if len(df_genders[gender].index) == 0:
            continue

        for occupation in OCCUPATIONS_LAST[gender]:
            selected, df_genders = second_stage_process_occupation(
                gender, df_genders, occupation, age_range, OA_area,
                first_selection, occupation_OAareas_2019
            )
            selections.append(selected)

    return selections, df_genders


def third_stage(df_genders):
    selections = []

    for gender in GENDERS:
        # TODO: Assumed use of len() here - check
        if len(df_genders[gender].index) == 0:
            continue

        # Initialise a variable to 0. This variable will take the value of
        # occupation_list
        list_value = 0
        # Iterate the dataframe with the remaining people to be assigned an
        # occupation:
        for idx_person_1, person_1 in df_genders[gender].iterrows():
            df_genders[gender].at[idx_person_1, "Occupation"] = (
                OCCUPATIONS_LAST[gender][list_value]
            )
            list_value += 1
            # If the list_value is greater than 8 (there are 9 categories (0-8)
            # ), then list_value is restarted to 0
            if list_value == 9:
                list_value = 0

        selections.append(df_genders[gender])

    return selections, df_genders


def process_OA_area(OA_area, df_persons_16_20, df_census_occupation):
    first_selections = []
    second_selections = []
    third_selections = []

    # Select the row of the df_occupation that is related to the selected OA
    # area:
    df_occupation_OAarea = df_census_occupation.loc[
        (df_census_occupation['geography'] == OA_area)]

    # Select the people in the OAarea from my synthetic population:
    df_persons = df_persons_16_20.loc[
        (df_persons_16_20['Area_OA_x'] == OA_area)]

    for age_range in AGE_RANGES:
        df_genders = {
            "male": df_persons.loc[(df_persons['Age'] >= age_range[0])
                                   & (df_persons['Age'] <= age_range[1])
                                   & (df_persons['Sex'] == GENDERS["male"])],
            "female": df_persons.loc[(df_persons['Age'] >= age_range[0])
                                     & (df_persons['Age'] <= age_range[1])
                                     & (df_persons['Sex'] == GENDERS["female"])]
        }

        total_2019 = len(df_genders["male"].index) + len(
            df_genders["female"].index)

        first_selection, df_genders, occupation_OAareas_2019 = first_stage(
            df_genders, age_range, OA_area, df_occupation_OAarea, total_2019
        )
        second_selection, df_genders = second_stage(
            df_genders, age_range, OA_area, first_selection,
            occupation_OAareas_2019
        )
        third_selection, df_genders = third_stage(df_genders)

        first_selections.extend(first_selection.values())
        second_selections.extend(second_selection)
        third_selections.extend(third_selection)

    return first_selections, second_selections, third_selection, df_genders


def process_OA_areas(AreaOA_list, df_persons_16_20,
                     df_census_occupation):

    first_selections = []
    second_selections = []
    third_selections = []

    for count, OA_area in enumerate(AreaOA_list, 1):
        print(f"Processing OA area: {OA_area} |",
              round((count / len(AreaOA_list)) * 100, 1), '%', end="\r")

        first_selection, second_selection, third_selection, df_genders = (
            process_OA_area(
                OA_area, df_persons_16_20, df_census_occupation
            )
        )

        first_selections.extend(first_selection)
        second_selections.extend(second_selection)
        third_selections.extend(third_selection)

    print("")
    print("Concatenating dataframes")
    df_first_selected = pd.concat(first_selections, axis=0, ignore_index=True)
    df_second_selected = pd.concat(second_selections, axis=0, ignore_index=True)
    df_third_selected = pd.concat(third_selections, axis=0, ignore_index=True)
    df_third_selected["Occupation"] = df_third_selected["Occupation"].astype("int8")

    return pd.concat([df_first_selected, df_second_selected, df_third_selected],
                     ignore_index=True)


def analysis(df, df_persons_16_20):
    for occupation in OCCUPATION_CODES:
        print(f"Analysis for occupation code: {occupation}")

        df2 = df.loc[(df["Occupation"] == occupation)]
        print(f"Total: {(len(df2) / len(df_persons_16_20)) * 100} %")

        df_male = df2.loc[(df["Sex"] == 1)]
        print(f"Male: {(len(df_male) / len(df)) * 100} %")

        df_female = df2.loc[(df["Sex"] == 2)]
        print(f"Female: {(len(df_female) / len(df)) * 100} %")


def main():
    start_time = time.time()

    print("Loading dataframes from csv")
    df_households = pd.read_csv(HOUSEHOLDS_PATH, index_col=None, header=0)
    df_employed = pd.read_csv(EMPLOYED_PATH, index_col=None, header=0)
    df_unemployed = pd.read_csv(UNEMPLOYED_PATH, index_col=None, header=0)
    df_census_occupation = pd.read_csv(CENSUS_OCCUPATION_PATH, index_col=None,
                                       header=0)

    df_persons_16_20 = get_occupation_dataframe(df_employed, df_unemployed)
    AreaOA_list = get_AreaOA_list(df_households)
    df = process_OA_areas(AreaOA_list, df_persons_16_20, df_census_occupation)
    analysis(df, df_persons_16_20)

    print("Exporting to: ", OCCUPATION_PATH)
    df_employed.to_csv(OCCUPATION_PATH, encoding='utf-8', header=True)

    end_time = time.time()
    print(f"Economic activity finished in {round(end_time - start_time)} "
          f"seconds")


if __name__ == "__main__":
    main()