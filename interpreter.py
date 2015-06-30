#!/usr/bin/python

import sys,os,time,uuid,tarfile,ConfigParser
import json,couchdb,datetime,socket,random,shutil


def get_finished_calculations(db):
  map_fun = ''' function(doc) { if(doc.calculation.state=='finished') { emit(doc._id); } } '''
  results = db.query(map_fun)
  calculations = []
  for item in results:
    calculations.append(db.get(item.key))
  return calculations



def interpret_orca(log_file):
  results = dict()
  thermochemistry = dict()
  frequencies = dict()
  vib_frequencies = []
  f = log_file.read()
  output = f.split()
  number_of_atoms = 0
  for index, value in enumerate(output):
    # geometry
    if value == "Number" and output[index+1] == "of" and output[index+2] == "atoms":
        number_of_atoms = int(output[index+4])
    if value == "CARTESIAN" and output[index+1] == "COORDINATES" and output[index+2] == "(ANGSTROEM)":
      coordinates = []
      for j in range(0,number_of_atoms):
        coordinate = dict()
        coordinate['element'] = output[index+(j*4)+4]
        coordinate['x'] = float(output[index+(j*4)+5])
        coordinate['y'] = float(output[index+(j*4)+6])
        coordinate['z'] = float(output[index+(j*4)+7])
        coordinates.append(coordinate)
      results['coordinates'] = coordinates
    # electronic energy
    if value == "FINAL" and output[index+3] == "ENERGY":
      results['E'] = float(output[index+4])
    # thermochemistry

    # temperature
    if value == "Temperature" and output[index+1] == "...":
      thermochemistry['T'] = float(output[index+2])
    # pressure
    if value == "Pressure" and output[index+1] == "...":
      thermochemistry['P'] = float(output[index+2])
    # total mass
    if value == "Total" and output[index+1] == "Mass":
      thermochemistry['total_mass'] = float(output[index+3])
    # inner energy
    if value == "Electronic" and output[index+1] == "energy" and output[index+2] == "...":
      thermochemistry['E_el'] = float(output[index+3])
    if value == "Zero" and output[index+1] == "point" and output[index+2] == "energy":
      thermochemistry['E_zpe'] = float(output[index+4])
    if value == "Thermal" and output[index+1] == "vibrational" and output[index+2] == "correction":
      thermochemistry['E_vib'] = float(output[index+4])
    if value == "Thermal" and output[index+1] == "rotational" and output[index+2] == "correction":
      thermochemistry['E_rot'] = float(output[index+4])
    if value == "Thermal" and output[index+1] == "translational" and output[index+2] == "correction":
      thermochemistry['E_trans'] = float(output[index+4])
    if value == "Total" and output[index+1] == "thermal" and output[index+2] == "energy":
      thermochemistry['U'] = float(output[index+3])
    # enthalpy
    if value == "Thermal" and output[index+1] == "Enthalpy" and output[index+2] == "correction":
      thermochemistry['kB*T'] = float(output[index+4])
    if value == "Total" and output[index+1] == "Enthalpy":
      thermochemistry['H'] = float(output[index+3])
    # entropy
    if value == "Electronic" and output[index+1] == "entropy":
      thermochemistry['T*S_el'] = float(output[index+3])
    if value == "Vibrational" and output[index+1] == "entropy":
      thermochemistry['T*S_vib'] = float(output[index+3])
    if value == "Rotational" and output[index+1] == "entropy":
      thermochemistry['T*S_rot'] = float(output[index+3])
    if value == "Translational" and output[index+1] == "entropy":
      thermochemistry['T*S_trans'] = float(output[index+3])
    if value == "Final" and output[index+1] == "entropy" and output[index+2] == "term":
      thermochemistry['T*S'] = float(output[index+4])
    # free enthalpy
    if value == "Final" and output[index+1] == "Gibbs" and output[index+2] == "free" and output[index+3] == "enthalpy":
      thermochemistry['G'] = float(output[index+5])
    # # frequencies
    # # vibrational
    # if value == "VIBRATIONAL" and output[index+1] == "FREQUENCIES":
    #   for j in range(0,number_of_atoms*3):
    #     if output[(j*3)+(index+4)] != "0.00":
    #       vib_frequencies.append(float(output[(j*3)+(index+4)]))
  if thermochemistry != dict():
    results['thermochemistry'] = thermochemistry
  if vib_frequencies != []:
    frequencies['vibrational'] = vib_frequencies
  if frequencies != dict():
    results['frequencies'] = frequencies
  return results


# read settings
home_dir = os.path.expanduser("~")
config_file = open(home_dir + '/.config/ccdb/config.json','r')
settings = json.loads(config_file.read())



# database setup
host = settings['database']['host']
port = settings['database']['port']
db_name = settings['database']['database']
user = settings['database']['user']
password = settings['database']['password']



# connect to server and database
url = 'http://' + user + ':' + password + '@' + host + ':' + port + '/'
server = couchdb.Server(url)
db = server[db_name]



work_dir = settings['jobs']['directory']
orca = settings['programs']['orca']['path']
input_file = "job.inp"
output_file = "job.out"




utc_datetime = datetime.datetime.utcnow()


while True:

  calculation_interpreted = False
  finished_calculations = get_finished_calculations(db)

  # check for new jobs

  if len(finished_calculations) == 0:
    print "no calculation finished"
    time.sleep(10)

  else:
    for doc in finished_calculations:
      print doc['_id']
      log_file = db.get_attachment(doc,'log')
      results = dict()
      if doc['calculation']['program']['name'] == "orca":
        results = interpret_orca(log_file)
      doc['results'] = results
      doc['calculation']['state'] = "interpreted"
      db.save(doc)
