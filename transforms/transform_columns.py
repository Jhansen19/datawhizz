# 1. Load the CSV file into a DataFrame
# 2. Define the list of columns that should be in the new DataFrame
# 3. For each column in the list:
#    a. If the column is not present in the DataFrame, add it with NaN values
# 4. Rename the specified columns:
#    a. Rename 'Email_Address_1_Present_Flag' to 'Email_1_Present_Flag'
#    b. Rename 'Email_Address' to 'Email_1'
# 5. Create a new DataFrame 'Optin' with columns: 'ID', 'Url', 'Ip', 'Optin_Date', 'Domain'
# 6. Save the 'Optin' DataFrame to a CSV file
# 7. Drop the columns 'Url', 'Ip', 'Optin_Date', 'Domain' from the original DataFrame
# 8. Save the modified original DataFrame to a new CSV file

import pandas as pd
import numpy as np

# Load the CSV file into a DataFrame
input_file_path = r"C:\Users\jnthn\Documents\Data_Science\DataWhizz\Consumer_100000.csv"
df = pd.read_csv(input_file_path)

# Define the list of columns that should be in the new DataFrame
required_columns = [
    # Add the full list of required columns here, including those that might be missing
     'Id', 'Address_ID', 'Household_ID', 'First_Name', 'Middle_Name', 'Last_Name', 'Name_Suffix', 'Address', 
     'City', 'State', 'Zip', 'Zip4', 'Dpbc', 'Carrier_Route', 'Walk_Sequence_Code', 'Fips_State_Code', 
     'Fips_County_Code', 'County_Name', 'Latitude', 'Longitude', 'Address_Type', 'Msa', 'Cbsa', 'DMA', 
     'Census_Tract', 'Census_Block_Group', 'Census_Block', 'Census_Median_Home_Value', 'Census_Median_Hh_Income', 
     'Gender', 'Homeowner', 'Dob_Yr', 'Dob_Mon', 'Dob_Day', 'Exact_Age', 'Telephone_Landline_1', 
     'Telephone_Landline_1_DNC_Flag', 'Telephone_Landline_2', 'Telephone_Landline_2_DNC_Flag', 'Cellphone_1', 
     'Cellphone_1_Date', 'Cellphone_1_Carrier', 'Cellphone_1_DNC_Flag', 'Cellphone_2', 'Cellphone_2_Date', 
     'Cellphone_2_Carrier', 'Cellphone_2_DNC_Flag', 'Cellphone_3', 'Cellphone_3_DNC_Flag', 'Cellphone_3_Carrier', 
     'Cellphone_4', 'Cellphone_4_DNC_Flag', 'Cellphone_4_Carrier', 'Telephone_Work_1', 'Telephone_Work_1_DNC_Flag', 
     'Telephone_Work_2', 'Telephone_Work_2_DNC_Flag', 'Email_1_Present_Flag', 'Email_1', 'Hem_1', 'Maid_1', 'Email_2', 
     'Hem_2', 'Maid_2', 'Email_3', 'Hem_3', 'Maid_3', 'Email_4', 'Hem_4', 'Maid_4', 'Email_5', 'Hem_5', 'Maid_5', 'Hh_Income', 
     'Net_Worth', 'Credit_Lines', 'Education_Level', 'Occupation', 'Occ_Busn_Ownr', 'Num_Kids', 'Pres_Kids', 'Kids_M_0_2', 
     'Kids_F_0_2', 'Kids_U_0_2', 'Kids_M_3_5', 'Kids_F_3_5', 'Kids_U_3_5', 'Kids_M_6_10', 'Kids_F_6_10', 'Kids_U_6_10', 
     'Kids_M_11_15', 'Kids_F_11_15', 'Kids_U_11_15', 'Kids_M_16_17', 'Kids_F_16_17', 'Kids_U_16_17', 'Hh_Marital_Status', 
     'Length_Of_Residence', 'Dwell_Type', 'Num_Adults', 'Generations', 'Adults_M_18_24', 'Adults_F_18_24', 'Adults_U_18_24', 
     'Adults_M_25_34', 'Adults_F_25_34', 'Adults_U_25_34', 'Adults_M_35_44', 'Adults_F_35_44', 'Adults_U_35_44', 
     'Adults_M_45_54', 'Adults_F_45_54', 'Adults_U_45_54', 'Adults_M_55_64', 'Adults_F_55_64', 'Adults_U_55_64', 
     'Adults_M_65_74', 'Adults_F_65_74', 'Adults_U_65_74', 'Adults_M_75Plus', 'Adults_F_75Plus', 'Adults_U_75Plus', 
     'Buy_Mo_Buyer', 'Buy_Mo_Respdr', 'Buy_Ol_Purch_Ind', 'Buy_Mem_Clubs', 'Buy_Value_Priced', 'Buy_Wmns_Apparel', 
     'Buy_Wmns_Petite_Apparel', 'Buy_Wmns_Plus_Apparel', 'Buy_Young_Wmns_Apparel', 'Buy_Mns_Apparel', 'Buy_Mns_Big_Apparel', 
     'Buy_Young_Mns_Apparel', 'Buy_Kids_Apparel', 'Buy_Health_Beauty', 'Buy_Cosmetics', 'Buy_Jewelry', 'Buy_Luggage', 
     'Cc_Amex_Prem', 'Cc_Amex_Reg', 'Cc_Disc_Prem', 'Cc_Disc_Reg', 'Cc_Gas_Prem', 'Cc_Gas_Reg', 'Cc_Mc_Prem', 'Cc_Mc_Reg', 
     'Cc_Visa_Prem', 'Cc_Visa_Reg', 'Cc_Hldr_Bank', 'Cc_Hldr_Gas', 'Cc_Hldr_Te', 'Cc_Hldr_Unk', 'Cc_Hldr_Prem', 
     'Cc_Hldr_Ups_Dept', 'Cc_User', 'Cc_New_Issue', 'Cc_Bank_Cd_In_Hh', 'Invest_Act', 'Invest_Pers', 'Invest_Rl_Est', 
     'Invest_Stocks', 'Invest_Read_Fin_News', 'Invest_Money_Seekr', 'Int_Grp_Invest', 'Invest_Foreign', 'Invest_Est_Prop_Own', 
     'Int_Grp_Donor', 'Donr_Mail_Ord', 'Donr_Charitable', 'Donr_Animal', 'Donr_Arts', 'Donr_Kids', 'Donr_Wildlife', 
     'Donr_Environ', 'Donr_Health', 'Donr_Intl_Aid', 'Donr_Pol', 'Donr_Pol_Cons', 'Donr_Pol_Lib', 'Donr_Relig', 
     'Donr_Vets', 'Donr_Oth', 'Donr_Comm_Char', 'Vet_In_Hh', 'Int_Oth_Parenting', 'Single_Parent', 'Buy_Infant_Apparel', 
     'Buy_Kids_Learn_Toys', 'Buy_Kids_Baby_Care', 'Buy_Kids_School', 'Buy_Kids_Genl', 'Young_Adult_In_Hh', 'Sr_Adult_In_Hh', 
     'Int_Oth_Kids_Ints', 'Int_Oth_Grandkids', 'Int_Oth_Christian_Fam', 'Buy_Pets', 'Int_Oth_Pets_Cat', 'Int_Oth_Pets_Dog', 
     'Int_Oth_Pets_Oth', 'Int_Oth_Career_Imp', 'Occ_Working_Wmn', 'Occ_Afric_Amer_Prof', 'Occ_Soho_Ind', 'Int_Oth_Career', 
     'Buy_Magazines', 'Buy_Books', 'Buy_Audio_Books', 'Read_Avid', 'Read_Relig', 'Read_Scifi', 'Read_Magazines', 
     'Read_Audio_Books', 'Int_Grp_Read', 'Read_Hist_Mltry', 'Read_Curr_Affairs', 'Buy_Religious', 'Read_Science_Space', 
     'Mags', 'Ent_Educ_Ol', 'Ent_Gaming', 'Ent_Dvd_Videos', 'Ent_Tv_Video_Movie', 'Occ_Home_Off', 'Ent_Hi_End_Appl', 
     'Ent_Hdtv_Int', 'Ent_Stereo', 'Ent_Music_Playr', 'Ent_Music_Coll', 'Ent_Music_Avid', 'Ent_Movie_Coll', 
     'Ent_Cable_Tv', 'Ent_Video_Gaming', 'Ent_Sat_Dish', 'Ent_Computers', 'Ent_Pc_Gaming', 'Ent_Cons_Elec', 
     'Int_Grp_Movie_Music', 'Int_Grp_Elec', 'Int_Oth_Telcomm', 'Buy_Antiques', 'Buy_Art', 'Ent_Theater', 
     'Ent_Mus_Instr', 'Int_Coll_Genl', 'Int_Coll_Stamps', 'Int_Coll_Coins', 'Int_Coll_Arts', 'Int_Coll_Antiques', 
     'Int_Coll_Avid', 'Int_Grp_Coll', 'Int_Coll_Sports', 'Buy_Collectibles', 'Int_Hob_Diy', 'Int_Auto_Wk', 'Int_Hob_Sew', 
     'Int_Hob_Woodwk', 'Int_Hob_Photo', 'Int_Aviation', 'Int_Hob_House_Plant', 'Int_Hob_Crafts', 'Int_Hob_Gardening', 
     'Buy_Gardening', 'Buy_Home_Gard', 'Int_Grp_Home_Imp', 'Buy_Crafts', 'Buy_Photo_Video', 'Int_Oth_Smoking', 
     'Int_Oth_Home_Dec', 'Int_Hob_Home_Imp', 'Int_Oth_Home_Imp_Int', 'Int_Oth_Cook_Wine', 'Int_Oth_Cook_Gnrl', 
     'Int_Oth_Cook_Gourmet', 'Int_Oth_Cook_Nat_Food', 'Int_Grp_Cook', 'Int_Hob_Games', 'Int_Trav_Casino', 
     'Int_Hob_Sweeps', 'Int_Grp_Travel', 'Int_Trav_Genl', 'Int_Trav_Us', 'Int_Trav_Intl', 'Int_Trav_Cruise', 
     'Life_Home', 'Life_Diy', 'Life_Sporty', 'Life_Upscale', 'Life_Culture', 'Life_Highbrow', 'Life_Ht', 
     'Life_Common', 'Life_Prof', 'Life_Broader', 'Int_Grp_Exer', 'Int_Fit_Jog', 'Int_Fit_Walk', 'Int_Fit_Aerob', 
     'Int_Sport_Spect_Auto', 'Int_Sport_Spect_Tv_Sports', 'Int_Sport_Spect_Foot', 'Int_Sport_Spect_Base', 
     'Int_Sport_Spect_Bskt', 'Int_Sport_Spect_Hockey', 'Int_Sport_Spect_Soccer', 'Int_Sport_Tennis', 
     'Int_Sport_Golf', 'Int_Sport_Snow_Ski', 'Int_Sport_Mtrcycl', 'Int_Sport_Nascar', 'Int_Sport_Boating', 
     'Int_Sport_Scuba', 'Buy_Sport_Leis', 'Buy_Hunting', 'Int_Sport_Fishing', 'Int_Sport_Camp', 'Int_Sport_Shoot', 
     'Int_Grp_Sports', 'Int_Grp_Outdoor', 'Int_Fit_Health_Med', 'Int_Fit_Diet', 'Int_Fit_Self_Imp', 'Buy_Auto_Parts', 
     'Genl_Pp_Home_Value', 'Mr2_Amt', 'P2_Amt', 'P1_Dt', 'Mr_Loan_Typ', 'Mr2_Loan_Typ', 'P2_Loan_Typ', 'Mr_Lendr_Cd', 
     'Mr2_Lendr_Cd', 'P1_Lendr_Cd', 'Mr2_Lendr', 'Mr2_Rate_Typ', 'P2_Rate_Typ', 'Mr_Rate', 'Mr2_Rate', 'Prop_Bld_Yr', 
     'Prop_Ac', 'Prop_Pool', 'Prop_Fuel', 'Prop_Sewer', 'Prop_Water', 'Genl_Loan_To_Value', 'Ethnic_Code', 'Ethnic_Conf', 
     'Ethnic_Grp', 'Ethnic_Lang', 'Ethnic_Relig', 'Ethnic_Hisp_Cntry', 'Ethnic_Assim', 'Credit_Rating', 'Addr_Primary', 
     'Addr_Pre', 'Addr_Street', 'Addr_Post', 'Addr_Suffix', 'Addr_Abrev', 'Addr_Secy', 'Prop_Type', 'Payday_Flag', 
     'Payday_Date_Stamp', 'Payday_Loan_Requested_Amt', 'Payday_Employer_Name', 'Payday_Employment_Status', 
     'Payday_Occupation', 'Payday_Military_Flag', 'Party_Affiliation', 'FIPS_CountyState'  
     # Using the new names directly
    # Include other columns as necessary
]
# Rename specific columns
df.rename(columns={
    'Email_Address_1_Present_Flag': 'Email_1_Present_Flag',
    'Email_Address': 'Email_1'
}, inplace=True)

# Add missing columns with NaN values if they are not present in the DataFrame
for col in required_columns:
    if col not in df.columns:
        df[col] = np.nan

# Create a new DataFrame 'Optin' with specific columns
optin_columns = ['Id', 'Url', 'Ip', 'Optin_Date', 'Domain']
optin_df = df[optin_columns].copy()

# Save the 'Optin' DataFrame to a new CSV file
optin_file_path = r"C:\Users\jnthn\Documents\Data_Science\DataWhizz\Output_files\Optin_test.csv"
optin_df.to_csv(optin_file_path, index=False)

# Drop the specified columns from the original DataFrame
df.drop(columns=['Url', 'Ip', 'Optin_Date', 'Domain'], inplace=True)

# Reorder the DataFrame columns according to the specified order
df = df[required_columns]

# Save the modified original DataFrame to a new CSV file
output_file_path = r"C:\Users\jnthn\Documents\Data_Science\DataWhizz\Output_files\Consumer_column_transform.csv"
df.to_csv(output_file_path, index=False)

print(f"Optin DataFrame saved to {optin_file_path}")
print(f"Modified original DataFrame saved to {output_file_path}")
