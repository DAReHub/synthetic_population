# TODO: Refactoring - Mostly refactored, could improve readibility of
#  generate_driving_licences function
# Running x
# TODO: Verify outputs
# TODO: Resolve comments

import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


def replace_RUC11_values(df):
    # Replace RUC11 values for the ones that appear in the nts9901.ods table
    # (Full car driving licence holders by gender, region and Rural-Urban
    # Classification1: 17 years old and over, England, 2002/03 and 2020)
    # (4 categories only Urban Conurbation, Urban city and town, Rural Village,
    # Hamlet and Isolated Dwelling, Rural town and fringe)
    # Urban Conurbation
    print("Replacing RUC11 values")

    df.loc[df.RUC11 == 'Urban major conurbation', 'RUC11'] = 'Urban_Conurbation'
    df.loc[df.RUC11 == 'Urban minor conurbation', 'RUC11'] = 'Urban_Conurbation'

    # Urban city and town
    df.loc[df.RUC11 == 'Urban city and town', 'RUC11'] = 'Urban_city_and_town'
    df.loc[df.RUC11 == 'Urban city and town in a sparse setting', 'RUC11'] = \
        'Urban_city_and_town'

    # Rural Village, Hamlet and Isolated Dwelling
    df.loc[df.RUC11 == 'Rural hamlets and isolated dwellings', 'RUC11'] = \
        'Rural_Village_Hamlet_and_Isolated_Dwelling'
    df.loc[df.RUC11 == 'Rural hamlets and isolated dwellings in a sparse setting', 'RUC11'] = \
        'Rural_Village_Hamlet_and_Isolated_Dwelling'
    df.loc[df.RUC11 == 'Rural village', 'RUC11'] = \
        'Rural_Village_Hamlet_and_Isolated_Dwelling'
    df.loc[df.RUC11 == 'Rural village in a sparse setting', 'RUC11'] = \
        'Rural_Village_Hamlet_and_Isolated_Dwelling'

    # Rural town and fringe
    df.loc[df.RUC11 == 'Rural town and fringe', 'RUC11'] = \
        'Rural_town_and_fringe'
    df.loc[df.RUC11 == 'Rural town and fringe in a sparse setting', 'RUC11'] = \
        'Rural_town_and_fringe'

    return df


def generate_driving_licences(df, age_range_list):
    # FORCE ONLY ONE PERSON PER HOUSEHOLD TO BE ASSIGNED A DRIVING LICENCE IF
    # HE/SHE LIVES IN A HOUSEHOLD WITH AT LEAST ONE CAR
    print("Generating driving licences")

    df["Driving_license"] = False

    # Create a dataframe only containing people older than 17 and with at least
    # one car in the hosuehold
    df_car_17plus = df.loc[(df['LC4202_C_CARSNO_x'] > 1) & (df['Age'] > 17)]
    df_car_17plus.sample(frac=1)

    # Create a dataframe containing only one person per household (based on
    # 'VALUES FROM NTS0201' PK value) with at least one car
    df_driving_licence_forced = df_car_17plus.drop_duplicates(
        subset='HID_AreaOA_x')

    # SELECT THE REMAINING DRIVING LICENCE TO PEOPLE BASED ON NATIONAL TRAVEL
    # SURVEY TABLES (NTS9901 AND NTS0201)
    # List with the gender values (1:male, 2:female)
    gender_list = [1, 2]

    # VALUES FROM NTS9901
    # TODO: Is this still needed?
    # Save in a list the different values of urban/rural areas
    # rural_urban_areas_names_list = ['Urban_Conurbation', 'Urban_city_and_town', 'Rural_town_and_fringe','Rural_Village_Hamlet_and_Isolated_Dwelling']

    # Values given by me to force to choose specific people when there is not a
    # car, one car and more than one car available in the household
    # TODO: Add custom input parameter
    household_car_weight_list = [0.2, 0.3, 0.5]

    # VALUES FROM NTS0201
    # TODO: Add custom input parameters
    # ADAPTED TO MATCH THE % IN THE NORTH EAST!!
    men_driving_percentage_list = [0.34, 0.65, 0.83, 0.89, 0.89, 0.90, 0.81]
    women_driving_percentage_list = [0.31, 0.54, 0.66, 0.74, 0.74, 0.70, 0.49]

    # Create an empty list where the small blocks of dataframes will be stored
    df_persons_temp = []
    df_dict = {}

    # TODO: Remove outer for loop and put inner within a function
    for gender in gender_list:
        df_gender = df.loc[(df['Sex'] == gender)]

        for count, age_range in enumerate(age_range_list):

            if gender == 1:
                driving_percentage = men_driving_percentage_list[count]
            elif gender == 2:
                driving_percentage = women_driving_percentage_list[count]
            else:
                # TODO: Raise an exception here?
                pass

            # Create a dataframe containing only the previous selected gender
            # and one specific rural/urban area
            df_dict[f"df_{gender}"] = df_gender

            # Create a dataframe containing only the previous selected gender
            # and rural/urban area with a specific range of age (e.g. 17-20)
            key = f"df_{gender}_{age_range[0]}_{age_range[1]}"
            df_dict[key] = df_dict[f"df_{gender}"].loc[
                ((df_dict[f"df_{gender}"]['Age'] >= age_range[0])) & (
                (df_dict[f"df_{gender}"]['Age'] <= age_range[1]))]

            # Dataframe containing only those people assigned a driving licence
            # (forced) in a specific range of age (e.g. 17-20), sex and
            # urban/rural location
            df_dict[key + "_forced"] = (
                df_driving_licence_forced.loc)[
                    (df_driving_licence_forced['Age'] >= age_range[0])
                    & (df_driving_licence_forced['Age'] <= age_range[1])
                    & (df_driving_licence_forced['Sex'] == gender)
                ]

            # Remove those forced people to have a driving licence and keep only
            # those who can be assigned a driving licence based on age, sex and
            # urban/rural location attribute values
            # Concatenate the dataframe containing all persons from the same
            # sex, urban/rural area and range of year, with the ones from these
            # categories that have been selected to get a driving licence
            df_dict[key + "_remaining"] = (
                pd.concat([df_dict[key], df_dict[key + "_forced"]]))

            # Remove duplicates and keep only those who were not assigned a
            # driving licence
            df_dict[key + "_remaining"] = (
                df_dict[key + "_remaining"].drop_duplicates(keep=False))

            # print(len(globals()[f"df_{gender}_{area}_{age_range[0]}_{age_range[1]}_remaining"]))

            # Split the previous dataframe in three: people with no car in the
            # household, people with one car and people with more than one
            car_states = ["nocar", "onecar", "onecarplus"]
            for c, i in enumerate(car_states, 1):
                df_dict[key + "_remaining_" + i] = (
                    df_dict[key + "_remaining"].loc[
                        df_dict[key + "_remaining"]['LC4202_C_CARSNO_x'] == c
                    ]
                )

            # Choose a specific number of people who live in households without
            # car based on values from household_car_weight_list MINUS the
            # number of forced people assigned with a driving licence previously
            # gropued by sex, age and urban/rural area (dataframe:
            # "df_persons_driving_licence_forced"), (just because they live in a
            # household with at least one car)
            # TODO: Turn into a function?
            base_df_len = len(df_dict[key])
            forced_df_len = len(df_dict[key + "_forced"])
            remaining_cars = [
                df_dict[key + "_remaining_nocar"],
                df_dict[key + "_remaining_onecar"],
                df_dict[key + "_remaining_onecarplus"]
            ]
            cars_chosen = []

            for i in range(len(household_car_weight_list)):
                # TODO: is there a better name for this variable? What is this
                #  formula?
                calculation1 = (base_df_len * household_car_weight_list[i] *
                                driving_percentage)
                calculation2 = int(round(calculation1 - forced_df_len, 0))

                # TODO: Added replace=True which will cause duplicates - is this
                #  ok, do/will duplicates need removing?
                if calculation2 > 0:
                    if round(calculation2 / 3, 0) <= len(remaining_cars[i]):
                        cars_chosen.append(remaining_cars[i].sample(
                            int(round(calculation1, 0))
                            - int(round(forced_df_len / 3, 0)), replace=True))
                    else:
                        cars_chosen.append(remaining_cars[i])
                else:
                    cars_chosen.append(pd.DataFrame())

            # TODO: Alternative solution to the above - better?
            # def assign_cars(df_dict, gender, age_range):
            #     car_dict = {"nocar": 1, "onecar": 2, "onecarplus": 3}
            #     for car_type, car_value in car_dict.items():
            #         key1 = f"df_{gender}_{age_range[0]}_{age_range[1]}_remaining"
            #         key2 = key1 + f"_{car_type}"
            #         df_dict[key2] = df_dict[key1].loc[
            #             df_dict[key1]['LC4202_C_CARSNO_x'] == car_value]
            #     return df_dict

            # Concatenate the previous selected people AND the forced people
            # THESE ARE THE ONES SELECTED BASED (N SEX, LOCATION AND AGE) AND
            # THE PREVIOUS FORCED ONES
            # THESE ARE THE ONES THAT WILL HAVE A DRIVING LICENCE (UP TO NOW)
            cars_chosen.append(df_dict[key + "_forced"])
            df_persons_driving_selection = pd.concat(cars_chosen)

            # Check the number of chosen people to be assigned and compared to
            # the value that should be reached. If the value is lower, the
            # remaining should be collected randomly from the dataframe that
            # contains people with the same gender and age range.
            if (int(round(len(df_persons_driving_selection), 0)) <
                    int(round((len(df_dict[key]) * driving_percentage), 0))):
                leftovers = (int(len(df_dict[key]) * driving_percentage)
                             - len(df_persons_driving_selection))

            # Concatenate the dataframe containing all persons from the same
            # sex, urban/rural area and range of year, with the ones from these
            # categories that have been selected to get a driving licence
            # (forced and chosen).
            df_with_duplicates = (
                pd.concat([df_dict[key], df_persons_driving_selection]))

            # Remove duplicates and keep only those who were not assigned a
            # driving licence.
            df_leftovers = df_with_duplicates.drop_duplicates(keep=False)

            # If leftover value is > 0, then new drivers (value of leftovers)
            # have to be selected and assigned then a driving licence.
            if int(len(df_leftovers)) > 0:
                df_leftovers_driving = df_leftovers.sample(int(leftovers),
                                                           replace=True)
                # TODO: Was getting an error before adding 'replace=True'. Use
                #  this (which will include duplicates) or use the below which
                #  will use the df with the min number?
                # df_leftovers_driving = df_leftovers.sample(min(int(leftovers), len(df_leftovers)))

                # Concatenate the dataframe containing all persons from the same
                # sex, urban/rural area and range of year, with the ones from
                # these categories that have been selected to get a driving
                # licence.
                df_leftovers_duplicates = (
                    pd.concat([df_leftovers, df_leftovers_driving]))

                # Remove duplicates and keep only those who were not assigned a
                # driving licence.
                df_no_driving_licence = df_leftovers_duplicates.drop_duplicates(
                    keep=False)

                # Concatenate this new drivers with the previous ones:
                df_persons_driving_selection = (pd.concat(
                    [df_persons_driving_selection, df_leftovers_driving]))

                # Update attribute value "Driving_licence" = True to those who
                # have been assigned a driving licence before.
                df_persons_driving_selection["Driving_license"] = True
            else:
                df_persons_driving_selection["Driving_license"] = True

            # Concatenate the df_persons_driving_selection and the 'leftovers'
            # that were not assigned a driving licence. This new dataframe
            # should have all persons of the specific gender, urban/rural area
            # and range of age as the orignal dataframe.
            df_persons = (pd.concat(
                [df_persons_driving_selection, df_no_driving_licence]))

            # Append the dataframe into the temporal list
            df_persons_temp.append(df_persons)

    # concatenate all persons (lists of the 'df_persons_NE_OA_HID_temp' list) in
    # one dataframe.
    df_after_driving = pd.concat(df_persons_temp, axis=0,
                                            ignore_index=True)

    # Create a new dataframe with only the people under 18 years old
    # (this group has not been considered before and has to be included (without
    # driving licence) in order to have the total number of people in the final
    # dataset)
    df_people_under18 = df.loc[(df['Age'] < 18)]

    # Concatenate previous dataframe generated with the people under 18:
    df_after_driving = pd.concat([df_after_driving, df_people_under18])

    print("Number of people with driving licence: ",
          len(df_after_driving.loc[df_after_driving['Driving_license'] == True])
          )

    return df_after_driving


def analysis_1(df):
    # ANALYSIS of people with/without driving licence and access to car in the
    # household
    print("ANALYSIS 1")

    df_car_and_driving_licence = len(df.loc[(df['Age'] > 17)
                                            & (df['LC4202_C_CARSNO_x'] > 1)
                                            & (df['Driving_license'] == True)])

    df_car_and_NOdriving_licence = len(df.loc[(df['Age'] > 17)
                                              & (df['LC4202_C_CARSNO_x'] > 1)
                                              & (df['Driving_license'] == False)])

    df_Nocar_and_driving_licence = len(df.loc[(df['Age'] > 17)
                                              & (df['LC4202_C_CARSNO_x'] == 1)
                                              & (df['Driving_license'] == True)])

    df_drivingLicence = len(df.loc[(df['Age'] > 17)
                                   & (df['Driving_license'] == True)])

    print("persons WITH driving license and at least one car in the household: ",
          df_car_and_driving_licence)
    print("persons WITHOUT driving license and at least one car in the household: ",
          df_car_and_NOdriving_licence)
    print('person WITH driving license but NO car in the household: ',
          df_Nocar_and_driving_licence)
    print('TOTAL people with driving license: ', df_drivingLicence)
    print('% persons WITH driving license and at least one car in the household: ',
          df_car_and_driving_licence / df_drivingLicence * 100)
    print('% person WITH driving license but NO car in the household: ',
          df_Nocar_and_driving_licence / df_drivingLicence * 100)


def analysis_2(df, age_range_list, sex, driving_licence):
    print("ANALYSIS 2")
    for age in age_range_list:
        if age[0] == 70:
            value = len(df.loc[(df['Sex'] == sex)
                               & (df['Age'] > age[0])
                               & (df['Driving_license'] == driving_licence)])
        else:
            value = len(df.loc[(df['Sex'] == sex)
                               & (df['Age'] >= age[0])
                               & (df['Age'] <= age[1])
                               & (df['Driving_license'] == driving_licence)])
        print("Age range: ", age, "Sex: ", sex, "Driving licence: ",
              driving_licence, "Value: ", value)


def analysis_3(df, sex, RUC11):
    print(f"ANALYSIS 3: Drivers within {RUC11}, sex: {sex}")
    print("Driving Licence: True, Value: ")
    print(len(df.loc[(df['Sex'] == sex)
                     & (df['RUC11'] == RUC11)
                     & (df['Driving_license'] == True)
                     & (df['Age'] > 17)]))
    print("Driving Licence: False, Value: ")
    print(len(df.loc[(df['Sex'] == sex)
                     & (df['RUC11'] == RUC11)
                     & (df['Driving_license'] == False)
                     & (df['Age'] > 17)]))
    print("Driving Licence: Not specified, Value: ")
    print(len(df.loc[(df['Sex'] == sex)
                     & (df['RUC11'] == RUC11)
                     & (df['Age'] > 17)]))


def analysis_4(df):
    print("ANALYSIS 5: With and without licence")
    print("With licence: ")
    print(len(df.loc[(df['Driving_license'] == True) & (df['Age'] > 17)]))
    print("Without licence: ")
    print(len(df.loc[(df['Driving_license'] == False) & (df['Age'] > 17)]))


def main(composition_path=os.getenv("composition_path"),
         rural_urban_OA_path=os.getenv("rural_urban_OA_path"),
         persons_driving_licence_path=os.getenv("persons_driving_licence_path")):

    df_composition = pd.read_csv(composition_path, index_col=None, header=0)
    df_rural_urban_OA = pd.read_csv(rural_urban_OA_path, index_col=None,
                                    header=0)

    # Join df_composition with df_Rural_Urban_OA in order to get the type of
    # rural/urban area per OA area
    df_composition = df_composition.merge(
        df_rural_urban_OA, left_on='Area_OA_x', right_on='OA11CD', how='left')

    # Remove unnecesary columns
    df_composition = df_composition.drop([
        'OA11CD', 'RUC11CD', 'BOUND_CHGIND', 'ASSIGN_CHGIND', 'ASSIGN_CHREASON'
    ], axis=1)

    # Calculate the number of different urban classifications values, and
    # recalculate after replacing RUC11 values
    df_composition['RUC11'].value_counts()
    df_composition = replace_RUC11_values(df_composition)
    df_composition['RUC11'].value_counts()

    # VALUES FROM NTS0201
    # List containg the range values of groups of age
    age_range_list = [(18, 20), (21, 29), (30, 39), (40, 49), (50, 59),
                      (60, 69), (70, 120)]

    df_after_driving = generate_driving_licences(df_composition, age_range_list)

    analysis_1(df_after_driving)
    analysis_2(df_after_driving, age_range_list, 1, True)
    analysis_2(df_after_driving, age_range_list, 1, False)
    analysis_2(df_after_driving, age_range_list, 2, True)
    analysis_2(df_after_driving, age_range_list, 2, False)
    analysis_3(df_after_driving, 1, "Urban_Conurbation")
    analysis_3(df_after_driving, 2, "Urban_Conurbation")
    analysis_3(df_after_driving, 1, "Rural_town_and_fringe")
    analysis_3(df_after_driving, 2, "Rural_town_and_fringe")
    analysis_4(df_after_driving)

    print("Exporting to: ", persons_driving_licence_path)
    df_after_driving.to_csv(persons_driving_licence_path, encoding='utf-8',
                            header=True)


if __name__ == "__main__":
    main()