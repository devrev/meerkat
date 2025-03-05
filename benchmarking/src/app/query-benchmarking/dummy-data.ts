interface TaxiTrip {
  Airport_fee: number;
  DOLocationID: number;
  PULocationID: number;
  RatecodeID: number;
  VendorID: number;
  congestion_surcharge: number;
  extra: number;
  fare_amount: number;
  improvement_surcharge: number;
  mta_tax: number;
  passenger_count: number;
  payment_type: number;
  store_and_fwd_flag: 'Y' | 'N';
  tip_amount: number;
  tolls_amount: number;
  total_amount: number;
  tpep_dropoff_datetime: string; // Unix timestamp in milliseconds
  tpep_pickup_datetime: string; // Unix timestamp in milliseconds
  trip_distance: number;
}

// For a collection of trips
interface TaxiTripsData {
  taxi_trips: TaxiTrip[];
}

export const tableName = 'in_memory_taxi_trips';

export const DUMMY_DATA: TaxiTripsData = {
  taxi_trips: [
    {
      Airport_fee: 0,
      DOLocationID: 79,
      PULocationID: 99,
      RatecodeID: 1,
      VendorID: 2,
      congestion_surcharge: 2.5,
      extra: 1,
      fare_amount: 17.7,
      improvement_surcharge: 1,
      mta_tax: 0.5,
      passenger_count: 1,
      payment_type: 2,
      store_and_fwd_flag: 'N',
      tip_amount: 0,
      tolls_amount: 0,
      total_amount: 22.7,
      tpep_dropoff_datetime: `'2023-01-01 14:30:00'`,
      tpep_pickup_datetime: `'2023-01-01 14:30:00'`,
      trip_distance: 1.72,
    },
    {
      Airport_fee: 0,
      DOLocationID: 236,
      PULocationID: 10,
      RatecodeID: 1,
      VendorID: 1,
      congestion_surcharge: 2.5,
      extra: 3.5,
      fare_amount: 10,
      improvement_surcharge: 1,
      mta_tax: 0.5,
      passenger_count: 1,
      payment_type: 1,
      store_and_fwd_flag: 'N',
      tip_amount: 3.75,
      tolls_amount: 0,
      total_amount: 18.75,
      tpep_dropoff_datetime: `'2023-01-01 14:30:00'`,
      tpep_pickup_datetime: `'2023-01-01 14:30:00'`,
      trip_distance: 1.8,
    },
  ],
};

export const createTableSQL = () => {
  return `CREATE TABLE IF NOT EXISTS ${tableName} (
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DECIMAL(10,2),
    RatecodeID INTEGER,
    store_and_fwd_flag VARCHAR(1),
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount DECIMAL(10,2),
    extra DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    Airport_fee DECIMAL(10,2),
    )`;
};

export const insertDataSQL = (taxiTrips: TaxiTrip[]) => {
  const values = taxiTrips
    .map(
      (trip) => `(
        ${trip.Airport_fee},
        ${trip.DOLocationID},
        ${trip.PULocationID},
        ${trip.RatecodeID},
        ${trip.VendorID},
        ${trip.congestion_surcharge},
        ${trip.extra},
        ${trip.fare_amount},
        ${trip.improvement_surcharge},
        ${trip.mta_tax},
        ${trip.passenger_count},
        ${trip.payment_type},
        '${trip.store_and_fwd_flag}',
        ${trip.tip_amount},
        ${trip.tolls_amount},
        ${trip.total_amount},
        ${trip.tpep_dropoff_datetime},
        ${trip.tpep_pickup_datetime},
        ${trip.trip_distance}
    )`
    )
    .join(',');
  const sql = `INSERT INTO ${tableName} 
    (
        Airport_fee,
        DOLocationID,
        PULocationID,
        RatecodeID,
        VendorID,
        congestion_surcharge,
        extra,
        fare_amount,
        improvement_surcharge,
        mta_tax,
        passenger_count,
        payment_type,
        store_and_fwd_flag,
        tip_amount,
        tolls_amount,
        total_amount,
        tpep_dropoff_datetime,
        tpep_pickup_datetime,
        trip_distance
    ) VALUES ${values}`;
  console.log(sql);
  return sql;
};
