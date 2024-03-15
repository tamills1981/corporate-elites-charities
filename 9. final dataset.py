import pandas as pd
from utils import PROJECT_DIR
from getters import get_CRCs

DATA_DIR = f'{PROJECT_DIR}/'

#Upload data
data_final = get_CRCs(DATA_DIR)

#Combine variables
data_final['makes_grants'] = data_final['how_makes_grants_to_individuals'] | data_final['how_makes_grants_to_organisations']
data_final['disability'] = data_final['what_disability'] | data_final['who_people_with_disabilities']

#Drop missing values and columns not wanted for analysis (inc. combined columns)
unwanted_columns = ['registered_charity_number', 'charity_company_registration_number',
'charity_contact_address1', 'charity_contact_address2',  
'charity_contact_address3', 'charity_contact_address4', 'charity_contact_address5',  
'charity_contact_postcode', 'charity_contact_phone', 'charity_contact_email',  
'charity_contact_web', 'charity_gift_aid', 'charity_has_land', 'mean_government_income',  
'mean_count_volunteers', 'charity_raises_funds_from_public', 'charity_professional_fundraiser',  
'charity_agreement_professional_fundraiser',  'charity_commercial_participator',  
'charity_agreement_commerical_participator', 'grant_making_is_main_activity',  
'charity_receives_govt_funding_contracts', 'charity_receives_govt_funding_grants',  
'charity_has_trading_subsidiary', 'trustee_also_director_of_subsidiary',  
'does_trustee_receive_any_benefit', 'trustee_payments_acting_as_trustee',  
'trustee_receives_payments_services', 'trustee_receives_other_benefit',  
'trustee_resigned_employment', 'employees_salary_over_60k', 'how_other_charitable_activities',
'how_acts_as_an_umbrella_or_resource_body', 'how_provides_services',
'how_provides_buildings/facilities/open_space', 'how_provides_human_resources',  
'how_provides_other_finance', 'what_general_charitable_purposes',  
'what_other_charitable_purposes', 'who_other_charities_or_voluntary_bodies',  
'who_other_defined_groups', 'how_makes_grants_to_individuals', 'how_makes_grants_to_organisations',
'what_disability', 'who_people_with_disabilities']

#Shorten some column names
data_final.rename(columns = {
'charity_activities': 'activities', 
'how_provides_advocacy/advice/information': 'advocacy/advice/info', 
'how_sponsors_or_undertakes_research': 'research', 
'what_accommodation/housing': ' accommodation/housing',
'what_amateur_sport': 'amateur_sport', 'what_animals': 'animals', 
'what_armed_forces/emergency_service_efficiency': 'armed_forces/emergency_services',
'what_arts/culture/heritage/science': 'arts/culture/heritage/science',
'what_economic/community_development/employment': 'economic/community_development/employment',
'what_education/training': 'education/training',
'what_environment/conservation/heritage': 'environment/conservation/heritage',
'what_human_rights/religious_or_racial_harmony/equality_or_diversity': 'human_rights/equality/diversity',
'what_overseas_aid/famine_relief': 'overseas_aid', 'what_recreation': 'recreation', 
'what_religious_activities': 'religion',
'what_the_advancement_of_health_or_saving_of_lives': 'health/saving_lives',
'what_the_prevention_or_relief_of_poverty': 'poverty', 
'who_children/young_people': 'young_people', 'who_elderly/old_people': 'old_people',
'who_people_of_a_particular_ethnic_or_racial_origin': 'ethnic_group',
'who_the_general_public/mankind':'public/mankind'}, inplace=True)

data_final = (
              data_final.drop(columns=unwanted_columns)
              .dropna()
              )

#Export CSV
data_final.to_csv(f'{DATA_DIR}/outputs/data_final.csv', index=False)

#File for analysis in Stata
for_stata = (
             data_final.replace({True: 1, False: 0})
             .drop(columns='activities')
             )

for_stata.to_csv(f'{DATA_DIR}/outputs/for_stata.csv', index=False)