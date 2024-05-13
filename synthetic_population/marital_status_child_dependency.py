# Refactored x
# Running - much faster than original x
# TODO: Debug - more ethnicities in households than there are people. Hundreds
#  of people in some households.
# TODO: Verify outputs - Outputs different than running original (and different
#  sizes) - which one is most accurate?. Original also suffers from issues
#  mentioned above: is there good reason for these issues or does logic need
#  work?

import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


def people_per_household(df_persons, df_households):
    # # Calculate for each person:
    # #     the total number in the household
    # #     The total number of children in the household
    # #     If there are more people with the same ethnicity in the household
    # #     If there is at least one more adult with a similar age
    #
    # # Create a list with all Households unique ID values
    # HID_AreaOA_list = df_households_NE['HID_AreaOA'].tolist()
    # print(len(HID_AreaOA_list))
    #
    # # Create an empty list where the small blocks of dataframes will be stored
    # df_persons_NE_OA_HID_temp = []
    #
    # for household_counter, HID_AreaOA in enumerate(HID_AreaOA_list, 1):
    #     print("Number of HOUSEHOLD in iteration: ", (household_counter, len(HID_AreaOA_list)))
    #
    #     # Get only the PERSONS that belong to the same HID_AreaOA
    #     df_persons_NE_OA_HID = df_persons_NE.loc[df_persons_NE['HID_AreaOA_x'] == HID_AreaOA]
    #     #print(df_persons_NE_OA_HID)
    #
    #     # Do the calculus just HOUSEHOLD BY HOUSEHOLD
    #     for idx_person_1, person_1 in df_persons_NE_OA_HID.iterrows():
    #         count_people = 1
    #
    #         if person_1['Age'] < 18:
    #             count_children = 1
    #         else:
    #             count_children = 0
    #
    #         for idx_person_2, person_2 in df_persons_NE_OA_HID.iterrows():
    #             if person_1['PID'] == person_2['PID']:
    #                 continue
    #
    #             count_people += 1
    #
    #             # If person_1 is older than 18 and the difference of age between
    #             # him/her and person_2 is below 10 years
    #             if person_2['Age'] < 18:
    #                 count_children += 1
    #             # TODO: Should the following not be >= 18?
    #             elif (person_1['Age'] > 18 and ((-10 <= person_2['Age'] - person_1['Age'] and person_2['Age'] - person_1['Age'] <= 10) or (-10 <= person_1['Age'] - person_2['Age'] and person_1['Age'] - person_2['Age'] <= 10 ))):
    #                 df_persons_NE_OA_HID.at[idx_person_1,'Adult_Similar_age'] = True
    #
    #             # If person_1 and person_2 have the same ethnic:
    #             if person_1['Ethnic'] == person_2['Ethnic']:
    #                 df_persons_NE_OA_HID.at[idx_person_1,'Same_ethnic'] = True
    #
    #         # Update values in the person's row
    #         df_persons_NE_OA_HID.at[idx_person_1,'Total_People_in_household'] = count_people
    #         df_persons_NE_OA_HID.at[idx_person_1,'Total_Children_in_household'] = count_children
    #
    #     # Append the dataframe into the temporal list
    #     df_persons_NE_OA_HID_temp.append(df_persons_NE_OA_HID)
    #
    # # concatenate all persons (lists of the 'df_persons_NE_OA_HID_temp' list) in
    # # one dataframe
    # df_persons_NE_Household_composition = pd.concat(df_persons_NE_OA_HID_temp,
    #                                                 axis=0, ignore_index=True)
    #
    # return df_persons_NE_Household_composition

    grouped = df_persons.groupby('HID_AreaOA_x')

    print('calculating total people and children in each household')
    df_persons['Total_People_in_household'] = grouped['PID'].transform('count')
    df_persons['Total_Children_in_household'] = grouped.apply(
        lambda x: (x['Age'] < 18).sum())

    print('checking if there are more people with the same ethnicity in the household')
    df_persons['Same_ethnic'] = grouped['Ethnic'].transform(
        lambda x: x.duplicated().any())

    print('checking if there is at least one more adult with a similar age in the household')
    df_persons['Adult_Similar_age'] = grouped.apply(
        lambda x: ((x['Age'] >= 18) & (x['Age'].diff().between(-10, 10))).any())

    return df_persons

    # Group by household
    # grouped = df_persons_NE.groupby('HID_AreaOA_x')
    #
    # print('calculating total people in each household')
    # df_persons_NE['Total_People_in_household'] = grouped['PID'].transform('count')
    #
    # print('calculating total children in each household')
    # df_persons_NE['Total_Children_in_household'] = grouped.apply(lambda x: (x['Age'] < 18).sum()).reset_index(name='Total_Children_in_household').set_index('HID_AreaOA_x')['Total_Children_in_household']
    #
    # print('checking if there are more people with the same ethnicity in the household')
    # df_persons_NE['Same_ethnic'] = grouped.apply(lambda x: x['Ethnic'].nunique() != 1).reset_index(name='Same_ethnic').set_index('HID_AreaOA_x')['Same_ethnic']
    #
    # print('checking if there is at least one more adult with a similar age in the household')
    # df_persons_NE['Adult_Similar_age'] = grouped.apply(lambda x: ((x['Age'] - x['Age'].shift()).abs() <= 10).any()).reset_index(name='Adult_Similar_age').set_index('HID_AreaOA_x')['Adult_Similar_age']
    #
    # return df_persons_NE


def marital_status(LC4408_C_AHTHUK11_x, Age, Adult_Similar_age):
    if LC4408_C_AHTHUK11_x == 2 and Age >= 18 and Adult_Similar_age is True:
        Marital_status = "Married"
    elif LC4408_C_AHTHUK11_x == 3 and Age >= 18 and Adult_Similar_age is True:
        Marital_status = "Couple"
    else:
        Marital_status = "Single"
    return Marital_status


def children_dependency(LC4408_C_AHTHUK11_x, Age, Total_Children_in_household):
    # if ((LC4408_C_AHTHUK11_x == 2
    #     or LC4408_C_AHTHUK11_x == 3
    #     or LC4408_C_AHTHUK11_x == 4)
    #         and Age >= 18
    #         and Total_Children_in_household > 0):
    #     Children_dependency = True
    # else:
    #     Children_dependency = False
    # return Children_dependency
    # Checks if LC4408_C_AHTHUK11_x is in set. Returns True or False.
    return (LC4408_C_AHTHUK11_x in {2, 3, 4} and Age >= 18 and
            Total_Children_in_household > 0)


def main(persons_path=os.getenv("persons_cleaned_path"),
         households_path=os.getenv("households_cleaned_path"),
         composition_path=os.getenv("df_composition_path")):

    # Read CSV file containing the MSOA and OA values
    df_persons = pd.read_csv(persons_path, index_col=None, header=0)
    df_households = pd.read_csv(households_path, index_col=None, header=0)

    df_persons["Total_People_in_household"] = 0
    df_persons["Total_Children_in_household"] = 0
    df_persons["Same_ethnic"] = False
    df_persons["Adult_Similar_age"] = False

    df_composition = people_per_household(df_persons, df_households)

    df_composition["Marital_status"] = ""
    df_composition["Children_dependency"] = False

    print("classifying marital status based on characteristics")
    # TODO: Output seems to all be 'single' ?
    df_composition['Marital_status'] = (
        df_composition.apply(
            lambda x: marital_status(
                x['LC4408_C_AHTHUK11_x'], x['Age'], x['Adult_Similar_age']
            ), axis=1
        )
    )

    print("identifying which adults have children dependencies")
    # TODO: Total_Children_in_household was manually set to 0 earlier,
    #  therefore the result of children_dependency function will always return
    #  False
    df_composition['Children_dependency'] = (
        df_composition.apply(
            lambda x: children_dependency(
                x['LC4408_C_AHTHUK11_x'], x['Age'],
                x['Total_Children_in_household']
            ), axis=1
        )
    )

    print("Exporting to: ", composition_path)
    df_composition.to_csv(composition_path, encoding='utf-8', header=True)


if __name__ == "__main__":
    main()