from flask import Blueprint, request, jsonify
from pymongo import MongoClient
from bson.objectid import ObjectId
from flask import Flask
from bson import json_util
from flask_caching import Cache

## convifgurando cache
config = {
    "DEBUG": True,          # some Flask specific configs
    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 300
}

app = Flask(__name__)


db = MongoClient('mongodb://localhost:27017')
collection = db.covid_data.covid #COLLECTION 


app.config.from_mapping(config)
cache = Cache(app) 

# -	Requisição para receber todos os dados de cada document; 
@app.route('/covid', methods=['GET'])
def get_all():
    covid = []
    for s in collection.find():
       
        covid.append({'iso_code' : s['iso_code'],'continent' : s['continent'],'location' : s['location'],'date' : s['date'],'total_cases':s['total_cases'],'new_cases':s['new_cases'],'new_cases_smoothed':s['new_cases_smoothed'],'total_deaths':s['total_deaths'],
         'new_deaths':s['new_deaths'],'new_deaths_smoothed':s['new_deaths_smoothed'],'total_cases_per_million':s['total_cases_per_million'],
         'new_cases_per_million':s['new_cases_per_million'],'new_cases_smoothed_per_million':s['new_cases_smoothed_per_million'],
         'total_deaths_per_million':s['total_deaths_per_million'],'new_deaths_per_million':s['new_deaths_per_million'],
         'new_deaths_smoothed_per_million':s['new_deaths_smoothed_per_million'],'reproduction_rate':s['reproduction_rate'],
         'icu_patients':s['icu_patients'],'icu_patients_per_million':s['icu_patients_per_million'],'hosp_patients':s['hosp_patients'],
         'hosp_patients_per_million':s['hosp_patients_per_million'],'weekly_icu_admissions':s['weekly_icu_admissions'],
         'weekly_icu_admissions_per_million':s['weekly_icu_admissions_per_million'],'weekly_hosp_admissions':s['weekly_hosp_admissions'],
         'weekly_hosp_admissions_per_million':s['weekly_hosp_admissions_per_million'],
         'new_tests':s['new_tests'],'total_tests':s['total_tests'],'total_tests_per_thousand':s['total_tests_per_thousand'],
         'new_tests_per_thousand':s['new_tests_per_thousand'],'new_tests_smoothed':s['new_tests_smoothed'],
         'new_tests_smoothed_per_thousand':s['new_tests_smoothed_per_thousand'],'positive_rate':s['positive_rate'],
         'tests_per_case':s['tests_per_case'],'tests_units':s['tests_units'],'total_vaccinations':s['total_vaccinations'],
         'people_vaccinated':s['people_vaccinated'],'people_fully_vaccinated':s['people_fully_vaccinated'],
         'new_vaccinations':s['new_vaccinations'],'new_vaccinations_smoothed':s['new_vaccinations_smoothed'],
         'total_vaccinations_per_hundred':s['total_vaccinations_per_hundred'],'people_vaccinated_per_hundred':s['people_vaccinated_per_hundred'],
         'people_fully_vaccinated_per_hundred':s['people_fully_vaccinated_per_hundred'],
         'new_vaccinations_smoothed_per_million':s['new_vaccinations_smoothed_per_million'],'stringency_index':s['stringency_index'],
         'population':s['population'],'population_density':s['population_density'],'median_age':s['median_age'],
         'aged_65_older':s['aged_65_older'],'aged_70_older':s['aged_70_older'],'gdp_per_capita':s['gdp_per_capita'],
         'extreme_poverty':s['extreme_poverty'],'cardiovasc_death_rate':s['cardiovasc_death_rate']})

    return jsonify({'result':covid})


#-	Requisição para deletar um documento
@app.route('/covid/<id>', methods=['DELETE'])
def delete(id):
    collection.delete_one({'_id' : ObjectId(id)})
    response = jsonify({'message' : 'Registro com id:' + id + 'foi deletado'})
    return response

#-	Requisição para mostrar a quantidade de documentos encontrados com os parâmetros: 
# data maior do que 'date', location, e iso code.
@app.route('/covid/<date>_<location>_<iso_code>', methods=['GET'])
@cache.cached(timeout=300)
def count(date,location,iso_code):
    s = collection.find( {'date' : {"$gt" : date} , 'location' : location , 'iso_code' : iso_code} ).count()
    return {'result' : s}


# -	Requisição para criar um novo documento; 
@app.route('/covid', methods=['POST'])
def create():
    iso_code = request.json['iso_code']
    continent = request.json['continent']
    location = request.json['location']
    date = request.json['date']
    total_cases = request.json['total_cases']
    new_cases = request.json['new_cases']
    new_cases_smoothed = request.json['new_cases_smoothed']
    total_deaths = request.json['total_deaths']
    new_deaths = request.json['new_deaths']
    new_deaths_smoothed = request.json['new_deaths_smoothed']
    total_cases_per_million = request.json['total_cases_per_million']
    new_cases_per_million = request.json['new_cases_per_million']
    new_cases_smoothed_per_million = request.json['new_cases_smoothed_per_million']
    total_deaths_per_million = request.json['total_deaths_per_million']
    new_deaths_per_million = ['new_deaths_per_million']
    new_deaths_smoothed_per_million = request.json['new_deaths_smoothed_per_million']
    reproduction_rate = request.json['reproduction_rate']
    icu_patients = request.json['icu_patients']
    icu_patients_per_million = request.json['icu_patients_per_million']
    hosp_patients = request.json['hosp_patients']
    hosp_patients_per_million = request.json['hosp_patients_per_million']
    weekly_icu_admissions = request.json['weekly_icu_admissions']
    weekly_icu_admissions_per_million = ['weekly_icu_admissions_per_million']
    weekly_hosp_admissions = request.json['weekly_hosp_admissions']
    weekly_hosp_admissions_per_million = request.json['weekly_hosp_admissions_per_million']
    new_tests = request.json['new_tests']
    total_tests = request.json['total_tests']
    total_tests_per_thousand = request.json['total_tests_per_thousand']
    new_tests_per_thousand = request.json['new_tests_per_thousand']
    new_tests_smoothed = request.json['new_tests_smoothed']
    new_tests_smoothed_per_thousand = request.json['new_tests_smoothed_per_thousand']
    positive_rate = request.json['positive_rate']
    tests_per_case = request.json['tests_per_case']
    tests_units = request.json['tests_units']
    total_vaccinations = request.json['total_vaccinations']
    people_vaccinated = request.json['people_vaccinated']
    people_fully_vaccinated = request.json['people_fully_vaccinated']
    new_vaccinations = request.json['new_vaccinations']
    new_vaccinations_smoothed = request.json['new_vaccinations_smoothed']
    total_vaccinations_per_hundred = request.json['total_vaccinations_per_hundred']
    people_vaccinated_per_hundred = request.json['people_vaccinated_per_hundred']
    people_fully_vaccinated_per_hundred = request.json['people_fully_vaccinated_per_hundred']
    new_vaccinations_smoothed_per_million = request.json['new_vaccinations_smoothed_per_million']
    stringency_index = request.json['stringency_index']
    population = request.json['population']
    population_density = request.json['population_density']
    median_age = request.json['median_age']
    aged_65_older = request.json['aged_65_older']
    aged_70_older = request.json['aged_70_older']
    gdp_per_capita = request.json['gdp_per_capita']
    extreme_poverty = request.json['extreme_poverty']
    cardiovasc_death_rate = request.json['cardiovasc_death_rate']

    if iso_code and continent and location and date and population and population_density and median_age and aged_65_older and aged_70_older and gdp_per_capita and cardiovasc_death_rate:
        collection.insert(
            {'iso_code' : iso_code ,'continent' : continent,'location' : location,'date' : date,'total_cases': total_cases,'new_cases':new_cases,'new_cases_smoothed':new_cases_smoothed,'total_deaths':total_deaths,
             'new_deaths':new_deaths,'new_deaths_smoothed':new_deaths_smoothed,'total_cases_per_million':total_cases_per_million,
             'new_cases_per_million':new_cases_per_million,'new_cases_smoothed_per_million':new_cases_smoothed_per_million,
             'total_deaths_per_million':total_deaths_per_million,'new_deaths_per_million':new_deaths_per_million,
             'new_deaths_smoothed_per_million':new_deaths_smoothed_per_million,'reproduction_rate':reproduction_rate,
             'icu_patients':icu_patients,'icu_patients_per_million':icu_patients_per_million,'hosp_patients':hosp_patients,
             'hosp_patients_per_million':hosp_patients_per_million,'weekly_icu_admissions':weekly_icu_admissions,
             'weekly_icu_admissions_per_million':weekly_icu_admissions_per_million,'weekly_hosp_admissions':weekly_hosp_admissions,
             'weekly_hosp_admissions_per_million':weekly_hosp_admissions_per_million,
             'new_tests':new_tests,'total_tests':total_tests,'total_tests_per_thousand':total_tests_per_thousand,
             'new_tests_per_thousand':new_tests_per_thousand,'new_tests_smoothed':new_tests_smoothed,
             'new_tests_smoothed_per_thousand':new_tests_smoothed_per_thousand,'positive_rate':positive_rate,
             'tests_per_case':tests_per_case, 'tests_units':tests_units,'total_vaccinations':total_vaccinations,
             'people_vaccinated':people_vaccinated,'people_fully_vaccinated':people_fully_vaccinated,
             'new_vaccinations':new_vaccinations,'new_vaccinations_smoothed':new_vaccinations_smoothed,
             'total_vaccinations_per_hundred':total_vaccinations_per_hundred,'people_vaccinated_per_hundred':people_vaccinated_per_hundred,
             'people_fully_vaccinated_per_hundred':people_fully_vaccinated_per_hundred,
             'new_vaccinations_smoothed_per_million':new_vaccinations_smoothed_per_million,'stringency_index':stringency_index,
             'population':population,'population_density':population_density,'median_age':median_age,
             'aged_65_older':aged_65_older,'aged_70_older':aged_70_older,'gdp_per_capita':gdp_per_capita,
             'extreme_poverty':extreme_poverty,'cardiovasc_death_rate':cardiovasc_death_rate
            }
        )
        return {'message' : 'registro adicionado'}
    else: return {'message' : 'Houve algum erro'}


# -	Requisição para encontrar um documento com o parâmetro 'population' 
# (e retornar erro descrito em caso de erro)
@app.route('/covid/<population>', methods=['GET'])
@cache.cached(timeout=300)
def get_one(population):
    
    s = collection.find_one({'population' : population})
    response = json_util.dumps(s)
    if s:
        return response
    else:
        return{"messagem" : "Valor nao encontrado"}
    


if __name__ == '__main__':
    app.run(debug=True)