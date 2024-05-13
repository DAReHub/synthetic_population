import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


def sample_dataframe(df, value):
    df_len = len(df.index)
    if df_len == 0:
        return pd.DataFrame()
    # If df is less than value, all of the df is applied as a sample
    return df.sample(min(value, df_len))


def process_OA_area(OA_area, df_economic_activity, df_composition, gender_val,
                    age_range, conversor, df_potential, activity_status,
                    df_NO_inactive=None):
    # Select the row of the df_economic_activity that is related to the selected
    # OA area:
    df_economic_activity_area = df_economic_activity.loc[
        (df_economic_activity['geography'] == OA_area)]

    # Select ALL people that live in the selected OA area and are >= 16 years old:
    df_people_OA_area = df_composition.loc[(df_composition['Area_OA_x'] == OA_area)
                                           & (df_composition['Age'] >= 16)]

    # Select only those that belong to a specific gender (male of female) and
    # age range. The INACTIVE people will be chosen from this dataset. The
    # number of them will depend on the value of variable "globals()[f"inactive_
    # {gender}_{age_range[0]}_{age_range[1]}_2019"]"
    df = df_people_OA_area.loc[
        (df_people_OA_area['Sex'] == gender_val)
        & (df_people_OA_area['Age'] >= age_range[0])
        & (df_people_OA_area['Age'] <= age_range[1])
    ]

    # Calculate the total number of people with the specific gender and range of
    # age living in that OA area:
    total_in_OA = len(df.index)

    # Create a variable per type of economic activity based on sex and range of
    # age
    filter_cols = str(gender_val) + "_" + str(age_range[0]) + "_" + str(age_range[1])

    # Identify the number of people based on sex and range of age in the
    # selected OA area that are employed
    # Value from 2011!!
    # TODO: Value coming table LC6107EW that has to be updated to 2019
    employment_status = ["Employed", "Unemployed", "Inactive"]
    employment_dict = {}
    for status in employment_status:
        employment_dict[status] = df_economic_activity_area.iloc[
            0, df_economic_activity_area.columns.get_loc(filter_cols+"_"+status)
        ]

    # Value with the total number of people living in the seleted OA area (table
    # LC6107EW)
    total = sum(employment_dict.values())

    # Ratio of people 2019 vs 2011:
    # If the number of people of the selected gender and range of age is greater
    # than 0 (e.g., there are people in the OA with this specific sex and range
    # of age). If value > 1, it means there are more people in 2019 in the
    # selected OA area of the specific gender and range of age. If there are no
    # people (gender and age range) in the OA area, then the ratio will be equal
    # to 1.
    if total > 0:
        ratio_people_2019_2011 = total_in_OA / total
    else:
        ratio_people_2019_2011 = 1

    # New value for 2019 = (Value from 2011 (table LC6107EW)) * (inactive
    # conversor value (based on age range and sex)) * (ratio of people 2019 vs
    # 2011 (c)). This value is the number of people that will be randomly
    # assigned "inactive" based on their OA area, range of age and sex.
    value_2019 = int(round(employment_dict[activity_status] * conversor
                              * ratio_people_2019_2011, 0))

    if activity_status == "Inactive":
        # SELECT FIRST STUDENTS
        df_students = df.loc[(df['LC4605_C_NSSEC_x'] == 9)]

        # If there are STUDENTS in the OA area:
        # TODO: Refactored logic - test
        df_inactive_students = sample_dataframe(df_students, value_2019)

        # Remaining NUMBER of people to be assigned as INACTIVE:
        remaining_inactive_2019 = value_2019 - len(df_inactive_students)

        # Force remaining people to be INACTIVE where NSSEC value is null or 9, and
        # remove previously selected students in df_inactive_students.

        # Dataframe containing people of the selected OA area, sex, range of age and
        # nssec null.
        df_potential_inactive_NSCCnull = df_potential.loc[
            (df_potential['Area_OA_x'] == OA_area)
            & (df_potential['Sex'] == gender_val)
            & (df_potential['Age'] >= age_range[0])
            & (df_potential['Age'] <= age_range[1])
            & (df_potential['NSSEC'].isnull())]

        # Dataframe containing people of the selected OA area, sex, range of age and
        # nssec 9 (student)
        df_potential_inactive_NSCC9 = df_potential.loc[
            (df_potential['Area_OA_x'] == OA_area)
            & (df_potential['Sex'] == gender_val)
            & (df_potential['Age'] >= age_range[0])
            & (df_potential['Age'] <= age_range[1])
            & (df_potential['NSSEC'] == 9)]

        # This dataframe contains the people of the selected OA area, sex, range of
        # age and nssec (null or 9) that will be used to select the remaining
        # INACTIVE people. But before that, the previous selected students
        # df_inactive_students MUST be removed.
        df_potential_inactive = pd.concat([
            df_potential_inactive_NSCCnull, df_potential_inactive_NSCC9])

        # Remaining PEOPLE in the dataframe (df_gender_age0_age1 - (number of
        # students already selected)):
        # Concatenate previous selected students with the "potential inactives" and
        # remove duplicates
        df_potential_inactive_plus_students = pd.concat([
            df_potential_inactive, df_inactive_students])
        df_potential_inactive_remaining = df_potential_inactive_plus_students.drop_duplicates(
            keep=False)

        # Now, we are ready to select the remaining INACTIVE people:
        # Select randomly the number of people to be inactive based on age and sex:
        # TODO: Refactored logic - test
        num_remaining_inactive = min(remaining_inactive_2019, len(df_potential_inactive_remaining))
        df_inactive = df_potential_inactive_remaining.sample(
            num_remaining_inactive) if num_remaining_inactive > 0 else pd.DataFrame()

        # If there are still some people to be assigned as INACTIVE, then their
        # NSSEC can be any value.
        # Remaining NUMBER of people to be assigned as INACTIVE:
        second_remaining_inactive_2019 = (value_2019 - len(df_inactive_students)
                                          - len(df_inactive))

        # Concatenate all people of the specific sex, range age, OA area with the
        # selected students and the others wich NSSEC value is null or 9
        concat_list = [df, df_inactive_students, df_inactive]
        df_potential_inactive_plus_students_plus_null = pd.concat(concat_list)

        # Remove duplicates
        df_potential_inactive_remaining_last = df_potential_inactive_plus_students_plus_null.drop_duplicates(
            keep=False)

        # Select randomly the number of people to be inactive based on age and sex:
        # TODO: Refactored logic - test
        df_inactive_last = sample_dataframe(
            df_potential_inactive_remaining_last, second_remaining_inactive_2019
        )

        # Concatenate and return all persons "inactive" in one dataframe
        return pd.concat([df_inactive_students, df_inactive, df_inactive_last])

    elif activity_status == "Employed":
        # Select those people of the selected sex, age range and OA area that
        # can be selected:
        df_potential_employed = df_potential.loc[
            (df_potential['Sex'] == gender_val)
            & (df_potential['Age'] >= age_range[0])
            & (df_potential['Age'] <= age_range[1])
            & ((df_potential['Area_OA_x'] == OA_area))]

        # Select randomly the number of people to be employed based on age and
        # sex:
        # TODO: Refactored logic - test
        # TODO: Is 'df' here not meant to be 'df_potential_employed' like for
        #  taking other dataframe samples? If so put into sample_dataframe
        #  function
        if len(df.index) == 0:
            df_employed = pd.DataFrame()
        else:
            # If df_potential_employed is less than value_2019, all of it is
            # applied as a sample
            num_samples = min(value_2019, len(df_potential_employed))
            df_employed = df_potential_employed.sample(num_samples)

        # If there are still some people in the OA area to be assigned as
        # "EMPLOYED" but there are no more people in the selected dataframe,
        # then we are going to consider as well those people wich NSSEC = 8
        if value_2019 <= len(df_employed.index):
            df_employed_leftovers = pd.DataFrame
        else:
            # Select people in the OA area (depending on the sex type and age
            # range)
            df_OAarea_all = df_NO_inactive.loc[
                (df_NO_inactive['Sex'] == gender_val)
                & (df_NO_inactive['Area_OA_x'] == OA_area)
                & (df_NO_inactive['Age'] >= age_range[0])
                & (df_NO_inactive['Age'] <= age_range[1])]

            # Concatenate the selected employed with the whole people >=16 in
            # the OA area (depending on the sex type)
            df_NO_inactive_plus_employed = (pd.concat([df_OAarea_all, df_employed]))
            df_NO_inactive_plus_remaining = df_NO_inactive_plus_employed.drop_duplicates(
                subset="PID_AreaMSOA", keep=False)

            # TODO: Refactored logic - test
            df_employed_leftovers = sample_dataframe(
                df_NO_inactive_plus_remaining, value_2019 - len(df_employed))

        # Concatenate and return the selected dataframes with the selected
        # students
        return pd.concat([df_employed, df_employed_leftovers])


def converge(rate, conversor, AreaOA_list, df_economic_activity, df_composition,
             gender_val, age_range, df_potential, activity_status,
             df_NO_inactive=None):

    iteration_counter = 0
    total_percentage = 0
    dfs = []

    # TODO: Range of +/-1% used here although comments for employed suggest +-2% ?
    while abs(rate - total_percentage) > 1:
        iteration_counter += 1
        print("Iteration: ", iteration_counter, ' | CONVERSOR Value: ', conversor)

        for count, OA_area in enumerate(AreaOA_list, 1):
            length = len(AreaOA_list)
            print("Processing OA areas: ", round((count / length) * 100, 1),
                  "%", end="\r")
            dfs.append(
                process_OA_area(
                    OA_area, df_economic_activity, df_composition, gender_val,
                    age_range, conversor, df_potential, activity_status,
                    df_NO_inactive
                )
            )

        # Concatenate all persons in one dataframe
        df = pd.concat(dfs, axis=0, ignore_index=True)
        print(f"Number of people selected {activity_status} by age range and "
              f"gender: {len(df.index)}")

        # Calculate the TOTAL number of people with the same sex and range of
        # age:
        # TODO: I think it is correct to place df_composition here - original as
        #  unreferenced variable "df_persons_NE_Household_composition_updated_CORRECT"
        total = len(
            df_composition.loc[
                (df_composition['Sex'] == gender_val)
                & (df_composition['Age'] >= age_range[0])
                & (df_composition['Age'] <= age_range[1])
            ]
        )
        print('Total number of people age range and gender: ', total)

        # Calculate the % of people inactive with the same sex and range of age:
        total_percentage = len(df) / total * 100

        # Compare the results against the ones given in table Regional labour
        # market statistics:HI01 Headline indicators for the North East related
        # to year 2019. If differences obtained against data given is within 1%,
        # then it is Ok
        if (((rate - 1) <= total_percentage) & ((rate + 1) >= total_percentage)):
            print('The value is within the tolerance of 1%')
            print('Value obtained: ', total_percentage)
            print('Continuing with the other gender or age range')
            return df

        # If the difference is greater than a 1% (+/-) then a new iteration
        # should be done updating the parameter that transform the employment
        # rate from 2011 to 2019. If the difference is negative, then a POSITIVE
        # increment has to be added. If the difference is positive, then a
        # NEGATIVE increment has to be added.
        else:
            print('The % needs to be adjusted in another iteration')
            print('Value obtained: ', total_percentage)
            if (total_percentage - rate - 1) < 0:
                conversor += 0.025
            else:
                conversor -= 0.025

            print('NEW CONVERSOR Value is: ', conversor)
            continue


def process_activity_status(genders, age_range_list, AreaOA_list,
                            df_economic_activity, df_composition,
                            activity_status, rates, conversors,
                            df_NO_inactive=None):

    if activity_status == "Employed":
        df_potential = df_NO_inactive.loc[(df_NO_inactive["NSSEC"] != 8)]
    elif activity_status == "Inactive":
        df_potential = pd.concat([
            df_composition.loc[(df_composition['NSSEC'].isnull())],
            df_composition.loc[(df_composition['NSSEC'] == 9)]])

    dfs = []

    for gender in genders:
        for i, age_range in enumerate(age_range_list):
            print(f"Processing — Activity status: {activity_status}, Gender: "
                  f"{gender}, Age range: {age_range[0]}-{age_range[1]}")

            rate = rates[gender][i]
            conversor = conversors[gender][i]

            dfs.append(
                converge(
                    rate, conversor, AreaOA_list, df_economic_activity,
                    df_composition, genders[gender], age_range, df_potential,
                    activity_status, df_NO_inactive
                )
            )

    return pd.concat(dfs)


def analysis(df1, df2, age_range, gender, gender_val, activity_status):
    # Number of people grouped by sex and age range:
    total = len(df1.loc[(df1['Age'] >= age_range[0])
                        & (df1['Age'] <= age_range[1])
                        & (df1['Sex'] == gender_val)])

    # Total number of people in the population grouped by sex and age range
    total_population = len(df2.loc[(df2['Age'] >= age_range[0])
                                   & (df2['Age'] <= age_range[1])
                                   & (df2['Sex'] == gender_val)])

    # Percentage of people grouped by sex and age range:
    print(f"Percentage of {activity_status} {gender}s between the ages of "
          f"{age_range[0]} and {age_range[1]}: ")
    if activity_status == "Unemployed":
        print((total / (total + total_population)) * 100)
    else:
        print((total / total_population) * 100)


def main(composition_path=os.getenv("composition_path"),
         households_path=os.getenv("households_cleaned_path"),
         economic_activity_path=os.getenv("economic_activity_path"),
         employed_path=os.getenv("employed_path"),
         inactive_path=os.getenv("inactive_path"),
         unemployed_path=os.getenv("unemployed_path")):

    df_composition = pd.read_csv(composition_path, index_col=None, header=0)
    df_households = pd.read_csv(households_path, index_col=None, header=0)
    df_economic_activity = pd.read_csv(economic_activity_path, index_col=None,
                                       header=0)

    # Create a new empty column for the Economic_activity (empty string)
    df_composition["Economic_activity"] = ""

    # PARAMS TODO: Enable input config for params and make documentation
    genders = {"male": 1, "female": 2}

    # List containing the range of ages
    # This values come from Regional labour market statistics: HI01 Headline indicators for the North East
    # link: https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/datasets/headlinelabourforcesurveyindicatorsforthenortheasthi01
    age_range_list = [(16, 24), (25, 34), (35, 49), (50, 64), (65, 120)]

    # TODO: Make the following input parameters
    inactive_rates = {
        "male": [38.3, 10.0, 9.1, 26.8, 90.3],
        "female": [44.5, 23.1, 19.3, 31.6, 94.8]
    }
    inactive_conversors = {
        "male": [1.103, 1.234, 0.907, 0.849, 0.971],
        "female": [1.123, 0.987, 0.885, 0.716, 0.983]
    }
    employed_rates = {
        "male": [51.9, 84.2, 87.8, 68.8, 9.6],
        "female": [48.6, 73.1, 77.9, 66.2, 5.1]
    }
    employed_conversors = {
        "male": [1.114, 1.084, 1.108, 1.052, 1.272],
        "female": [1.025, 1.070, 1.041, 1.236, 0.900]
    }

    ## LIST OF OA_AREAS that has been generated before
    # Create a list with all Households unique ID values
    AreaOA_list = list(set(df_households['Area_OA'].tolist()))

    df_inactive = process_activity_status(
        genders, age_range_list, AreaOA_list, df_economic_activity,
        df_composition, "Inactive", inactive_rates, inactive_conversors
    )

    # concatenate all persons "EMPLOYED" in one dataframe
    df_composition = pd.concat([df_composition, df_inactive])

    # Remove duplicates and keep only those who were not assigned a driving
    # licence
    df_NO_inactive = df_composition.drop_duplicates(keep=False)

    df_employed = process_activity_status(
        genders, age_range_list, AreaOA_list, df_economic_activity,
        df_composition, "Employed", employed_rates, employed_conversors,
        df_NO_inactive
    )

    # Remove the previous selected people and keep the remaining ones
    df_unemployed = pd.concat([df_NO_inactive, df_employed])

    # Remove duplicates and keep only those who were not selected as "Inactive"
    # or "Employed"
    df_unemployed = df_unemployed.drop_duplicates(subset='PID_AreaMSOA',
                                                  keep=False)

    # Analysis
    age_range_list.append([(16, 120), (16, 64)])
    for gender in genders:
        for age_range in age_range_list:
            analysis(df_inactive, df_composition, age_range, gender,
                     genders[gender], "Inactive")
            analysis(df_employed, df_composition, age_range, gender,
                     genders[gender], "Employed")
            analysis(df_unemployed, df_employed, age_range, gender,
                     genders[gender], "Unemployed")

    # Update the "Economic_activity" to each of the dataframes generated before:
    df_employed["Economic_activity"] = "Employed"
    df_inactive["Economic_activity"] = "Inactive"
    df_unemployed["Economic_activity"] = "Unemployed"

    print("Exporting to: ", employed_path)
    df_employed.to_csv(employed_path, encoding='utf-8', header=True)
    print("Exporting to: ", inactive_path)
    df_employed.to_csv(inactive_path, encoding='utf-8', header=True)
    print("Exporting to: ", unemployed_path)
    df_employed.to_csv(unemployed_path, encoding='utf-8', header=True)


if __name__ == "__main__":
    main()