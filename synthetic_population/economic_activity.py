import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


def generate_inactive_people(df):
    # Concatenate a new dataframe containing only those people which NSSEC value
    # is null or equal to 9 (students)
    return pd.concat([df.loc[(df['NSSEC'].isnull())],
                      df.loc[(df['NSSEC'] == 9)]])


def process_OA_area(OA_area, df_economic_activity, df_composition, gender,
                    age_range, inactive_conversor, df_potential_inactive):
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
        (df_people_OA_area['Sex'] == gender)
        & (df_people_OA_area['Age'] >= age_range[0])
        & (df_people_OA_area['Age'] <= age_range[1])
    ]

    # Calculate the total number of people with the specific gender and range of
    # age living in that OA area:
    total_in_OA_2019 = len(df.index)

    # Create a variable per type of economic activity based on sex and range of
    # age
    filter_cols = gender + "_" + str(age_range[0]) + "_" + str(age_range[1])

    # Identify the number of people based on sex and range of age in the
    # selected OA area that are employed
    # Value from 2011!!
    # TODO: Value comming table LC6107EW that has to be updated to 2019
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
        ratio_people_2019_2011 = total_in_OA_2019 / total
    else:
        ratio_people_2019_2011 = 1

    # New value for 2019 = (Value from 2011 (table LC6107EW)) * (inactive
    # conversor value (based on age range and sex)) * (ratio of people 2019 vs
    # 2011 (c)). This value is the number of people that will be randomly
    # assigned "inactive" based on their OA area, range of age and sex.
    inactive_2019 = int(round(employment_dict["Inactive"] * inactive_conversor
                              * ratio_people_2019_2011, 0))

    # SELECT FIRST STUDENTS
    df_students = df.loc[(df['LC4605_C_NSSEC_x'] == 9)]

    # If there are STUDENTS in the OA area:
    # TODO: Refactored logic - test
    if not df_students.empty:
        # Select randomly the number of people that will be assigned as inactive
        num_inactive = min(inactive_2019, len(df_students))
        df_inactive_students = df_students.sample(num_inactive)
    else:
        df_inactive_students = pd.DataFrame()

    # Remaining NUMBER of people to be assigned as INACTIVE:
    remaining_inactive_2019 = inactive_2019 - len(df_inactive_students)

    # Force remaining people to be INACTIVE where NSSEC value is null or 9, and
    # remove previously selected students in df_inactive_students.

    # Dataframe containing people of the selected OA area, sex, range of age and
    # nssec null.
    df_potential_inactive_NSCCnull = df_potential_inactive.loc[
        (df_potential_inactive['Area_OA_x'] == OA_area)
        & (df_potential_inactive['Sex'] == gender)
        & (df_potential_inactive['Age'] >= age_range[0])
        & (df_potential_inactive['Age'] <= age_range[1])
        & (df_potential_inactive['NSSEC'].isnull())]

    # Dataframe containing people of the selected OA area, sex, range of age and
    # nssec 9 (student)
    df_potential_inactive_NSCC9 = df_potential_inactive.loc[
        (df_potential_inactive['Area_OA_x'] == OA_area)
        & (df_potential_inactive['Sex'] == gender)
        & (df_potential_inactive['Age'] >= age_range[0])
        & (df_potential_inactive['Age'] <= age_range[1])
        & (df_potential_inactive['NSSEC'] == 9)]

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
    second_remaining_inactive_2019 = (inactive_2019 - len(df_inactive_students)
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
    length_df = len(df_potential_inactive_remaining_last)
    if length_df > 0:
        sample_size = min(second_remaining_inactive_2019, length_df)
        df_inactive_last = df_potential_inactive_remaining_last.sample(
            sample_size)
    else:
        df_inactive_last = pd.DataFrame()

    # Concatenate all persons "inactive" in one dataframe
    df_inactive_all_each_area = pd.concat([df_inactive_students, df_inactive,
                                           df_inactive_last])

    return df_inactive_all_each_area


def inactive_iterator(iteration_counter, total_inactive_percentage,
                      AreaOA_list, df_economic_activity, df_composition, gender,
                      age_range, persons_inactive, df_potential_inactive,
                      inactive_rate, inactive_conversor):

    # let's start assigning economic activities to the persons in the
    # synthetic population based on the OA level, age and sex
    while abs(inactive_rate - total_inactive_percentage) > 1:
        iteration_counter += 1

        print("It: ", iteration_counter, 'CONVERSOR Value: ',
              inactive_conversor)

        # Assign / Clear the dataframe everytime there is a need for a
        # new iteration.
        df_gender_inactive = pd.DataFrame()
        df_inactive_students = pd.DataFrame()
        df_inactive_last = pd.DataFrame()

        for count, OA_area in enumerate(AreaOA_list, 1):
            df_inactive_all_each_area = process_OA_area(
                OA_area, df_economic_activity, df_composition, gender,
                age_range, inactive_conversor, df_potential_inactive)

            # Append the dataframe into the dict
            dict_key = f"df_{gender}_{age_range[0]}_{age_range[1]}_inactive_all_each_area"
            persons_inactive[dict_key] = df_inactive_all_each_area

        # Concatenate all persons "inactive" in one dataframe
        df_inactive_all = pd.concat(
            persons_inactive.values(), axis=0, ignore_index=True)
        print('Number of people selected INACTIVE by age range and gender: ')
        print(len(df_inactive_all.index))

        # Calculate the TOTAL number of people with the same sex and
        # range of age:
        total = len(df_composition.loc[
                        (df_composition['Sex'] == gender)
                        & (df_composition['Age'] >= age_range[0])
                        & (df_composition['Age'] <= age_range[1])
                        ])
        print('Total number of people age range and gender: ', total)

        # Calculate the % of people inactive with the same sex and range
        # of age:
        total_inactive_percentage = len(df_inactive_all) / total * 100

        # Compare the results against the ones given in table Regional
        # labour market statistics:HI01 Headline indicators for the
        # North East related to year 2019. If differences obtained
        # against data given is within 1%, then it is Ok
        if (((inactive_rate - 1) <= total_inactive_percentage)
                & ((inactive_rate + 1) >= total_inactive_percentage)):
            print('The value is within the tolerance of 1%')
            print('Value obtained: ', total_inactive_percentage)
            print('Continuing with the other gender or age range')
            # TODO: Is this the correct df to return?
            return df_inactive_all

        # If the difference is greater than a 1% (+/-) then a new
        # iteration should be done updating the parameter that transform
        # the employment rate from 2011 to 2019
        else:
            print('The % needs to be adjusted in another iteration')
            print('Value obtained: ', total_inactive_percentage)
            # If the difference is negative, then a POSITIVE increment
            # has to be added
            if (total_inactive_percentage - inactive_rate - 1) < 0:
                # Update the value transform data from 2011 to 2019
                # (increase the value):
                inactive_conversor += 0.025
            # If the difference is positive, then a NEGATIVE increment
            # has to be added
            else:
                # Update the value transform data from 2011 to 2019 (reduce
                # the value):
                inactive_conversor -= 0.025

            print('NEW CONVERSOR Value is: ', inactive_conversor)
            continue


def inactive_analysis(df_inactive, df_composition, age_range, gender, gender_val):
    # Number of inactive people grouped by sex and age range:
    total_inactive = len(df_inactive.loc[(df_inactive['Age'] >= age_range[0])
                                         & (df_inactive['Age'] <= age_range[1])
                                         & (df_inactive['Sex'] == gender_val)])

    # Total number of people in the population grouped by sex and age range
    total_population = len(df_composition.loc[(df_composition['Age'] >= age_range[0])
                                              & (df_composition['Age'] <= age_range[1])
                                              & (df_composition['Sex'] == gender_val)])

    # Percentage of people inactive grouped by sex and age range:
    print(f"Percentage of inactive {gender}s between the ages of {age_range[0]}" \
          f" and {age_range[1]}: {(total_inactive / total_population) * 100}")


def inactive_processing(df_composition, df_economic_activity, genders,
                        age_range_list, AreaOA_list):

    df_potential_inactive = generate_inactive_people(df_composition)
    iteration_counter = 0
    inactive_list = []

    # TODO: Make the following input parameters
    # Inactive rates and conversors per gender per age range
    inactive_rates = {
        "male": [38.3, 10.0, 9.1, 26.8, 90.3],
        "female": [44.5, 23.1, 19.3, 31.6, 94.8]
    }
    inactive_conversors = {
        "male": [1.103, 1.234, 0.907, 0.849, 0.971],
        "female": [1.123, 0.987, 0.885, 0.716, 0.983]
    }

    for gender in genders:
        for i, age_range in enumerate(age_range_list):
            print('Processing: ', gender, str(age_range[0]), str(age_range[1]))

            inactive_rate = inactive_rates[gender][i]
            inactive_conversor = inactive_conversors[gender][i]

            # TODO: Instead of using 'persons_inactive_list.clear()' I've
            #  included persons_inactive list here to reset with each new
            #  iteration. Does this break anything globally? Also changed to
            #  dict.
            persons_inactive = {}
            total_inactive_percentage = 0

            # TODO: What does this need to return? Iteration counter at least
            inactive_list.append(
                inactive_iterator(
                    iteration_counter, total_inactive_percentage, AreaOA_list,
                    df_economic_activity, df_composition, gender, age_range,
                    persons_inactive, df_potential_inactive, inactive_rate,
                    inactive_conversor
                )
            )

    df_inactive = pd.concat(inactive_list)

    analysis_age_ranges = age_range_list.append((16, 120), (16, 64))

    for gender in genders:
        for age_range in analysis_age_ranges:
            inactive_analysis(df_inactive, age_range, gender, genders[gender])

    # TODO: Future feature - include some sort of visualisation outputs?

    print('Inactive processing and analysis complete. Please check results')




def main(composition_path=os.getenv("composition_path"),
         households_path=os.getenv("households_cleaned_path"),
         economic_activity_path=os.getenv("households_cleaned_path")):

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

    ## LIST OF OA_AREAS that has been generated before
    # Create a list with all Households unique ID values
    AreaOA_list = list(set(df_households['Area_OA'].tolist()))

    inactive_processing(df_composition, df_economic_activity, genders,
                        age_range_list, AreaOA_list)





if __name__ == "__main__":
    main()