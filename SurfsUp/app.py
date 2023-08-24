# Import the dependencies.

import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# reflect an existing database into a new model

Base = automap_base()


# reflect the tables

Base.prepare(autoload_with=engine)
Base.classes.keys()


# Save references to each table

Measurements = Base.classes.measurement
Stations = Base.classes.station


#################################################
# Flask Setup
#################################################

app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the last 12 months of precipitation data"""
   
    # Create our session (link) from Python to the DB
    session = Session(engine)

   
    # Calculate most recent date + one year prior date

    most_recent_date = session.query(Measurements.date).order_by(Measurements.date.desc()).first()

    one_year_prior_series = pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)
    
    one_year_prior = one_year_prior_series[0]


    # Query all measurement data from past 12 months + tranfer to DF then to dictionary

    measurements_query = session.query(Measurements).filter(Measurements.date > one_year_prior.date()).order_by(Measurements.date).all()

    measurements_df = pd.DataFrame([(mq.date, mq.prcp) for mq in measurements_query], columns=['date', 'precipitation'])

    measurements_dict = measurements_df.set_index('date')['precipitation'].to_dict()

    
    # Return results

    return jsonify(measurements_dict)


    # Close the session

    session.close()



@app.route("/api/v1.0/stations")
def stations():
    """Return list of stations"""
   
    # Create our session (link) from Python to the DB
    session = Session(engine)

   
    # Query all stations data + tranfer to DF then to dictionary

    stations_query = session.query(Stations).all()

    stations_df = pd.DataFrame([(sq.station, sq.name) for sq in stations_query], columns=['station_code', 'station_name'])

    stations_dict = stations_df.set_index('station_code')['station_name'].to_dict()

    
    # Return results

    return jsonify(stations_dict)


    # Close the session
    
    session.close()



@app.route("/api/v1.0/tobs")
def temperature():
    """Return the last 12 months of temperature observations from the most active station"""
   
    # Create our session (link) from Python to the DB
    session = Session(engine)

   
    # Calculate most recent date + one year prior date

    most_recent_date = session.query(Measurements.date).order_by(Measurements.date.desc()).first()

    one_year_prior_series = pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)
    
    one_year_prior = one_year_prior_series[0]


    # Determine which station has been most active over the past 12 months

    measurements_query2 = session.query(Measurements).all()

    new_measurement_df = pd.DataFrame([(mq.date, mq.tobs, mq.station) for mq in measurements_query2], columns=['date', 'temperature', 'station'])

    station_grouping_df = pd.DataFrame(new_measurement_df.groupby('station').size().sort_values(ascending=False)).reset_index()

    station_grouping_df.rename(columns={station_grouping_df.columns[1]:'count'},inplace=True)

    most_active_station = station_grouping_df['station'][0]


    # Query all measurement data from past 12 months for the most active station + tranfer to DF then to dictionary

    measurements_query = session.query(Measurements).filter(Measurements.date > one_year_prior.date()).filter(Measurements.station == most_active_station).order_by(Measurements.date).all()

    most_active_station_measurements_df = pd.DataFrame([(mq.date, mq.tobs) for mq in measurements_query], columns=['date', 'temperature'])

    most_active_station_measurements_dict = most_active_station_measurements_df.set_index('date')['temperature'].to_dict()

    
    # Return results

    return jsonify(most_active_station_measurements_dict)


    # Close the session

    session.close()



@app.route("/api/v1.0/<start>")
def temps_after(start):
    """Return minimum, average, and maximum temperatures from all dates on or after the <start> date"""
   
    # Create our session (link) from Python to the DB
    session = Session(engine)


    # Query all measurement data from on or after the <start> date + tranfer to DF

    measurements_query = session.query(Measurements).filter(Measurements.date >= start).order_by(Measurements.date).all()

    temperature_df = pd.DataFrame([(mq.date, mq.tobs) for mq in measurements_query], columns=['date', 'temperature'])
    
    temp_list = temperature_df['temperature'].tolist()

    min_temp = min(temp_list)
    max_temp = max(temp_list)
    avg_temp = sum(temp_list) / len(temp_list)

    results_dict = {'Minimum':min_temp,'Maximum':max_temp,'Average':avg_temp}


    # Return results

    return jsonify(results_dict)


    # Close the session

    session.close()




@app.route("/api/v1.0/<start>/<end>")
def temp_range(start,end):
    """Return minimum, average, and maximum temperatures from all dates on or after the <start> date"""
   
    # Create our session (link) from Python to the DB
    session = Session(engine)


    # Query all measurement data from on or after the <start> date + tranfer to DF

    measurements_query = session.query(Measurements).filter(Measurements.date >= start).filter(Measurements.date <= end).order_by(Measurements.date).all()

    temperature_df = pd.DataFrame([(mq.date, mq.tobs) for mq in measurements_query], columns=['date', 'temperature'])
    
    temp_list = temperature_df['temperature'].tolist()

    min_temp = min(temp_list)
    max_temp = max(temp_list)
    avg_temp = sum(temp_list) / len(temp_list)

    results_dict = {'Minimum':min_temp,'Maximum':max_temp,'Average':avg_temp}

    
    # Return results

    return jsonify(results_dict)


    # Close the session

    session.close()





if __name__ == '__main__':
    app.run(debug=True)



