create table room(
    id int primary key,
    description varchar(255) not null,
    room_identification varchar(255) not null unique
);

create table connection(
    id int primary key,
    description varchar(255) not null unique,
    is_virtual boolean,
    connection_identification varchar(255) not null unique
);

create table room_connection(
  id int primary key,
  room_id int,
  connection_id int
);

ALTER TABLE room_connection
    ADD CONSTRAINT fssafvaf
        FOREIGN KEY (room_id)
            REFERENCES room (id);

ALTER TABLE room_connection
    ADD CONSTRAINT skogvasdn
        FOREIGN KEY (connection_id)
            REFERENCES connection (id);

create table motion_sensor(
    id int primary key,
    description varchar(255) not null unique ,
    sensor_identification varchar(255) not null unique,
    time_stamp varchar(255) not null ,
    occupancy boolean not null,
    room_id int
);

ALTER TABLE motion_sensor
    ADD CONSTRAINT fadsfdas
        FOREIGN KEY (room_id)
            REFERENCES room (id);


create table vibration_sensor(
    id int primary key,
    description varchar(255),
    sensor_identification varchar(255),
    time_stamp varchar(255),
    temperature float,
    vibration boolean,
    angle_x int,
    angle_Y int,
    angle_z int,
    angle int,
    angle_x_absolute int,
    angle_y_absolute int,
    vibration_sensor_entity_id int

);

create table vibration_sensor_entity(
    id int primary key,
    description varchar(255),
    vibration_sensor_entity_identification varchar(255),
    room_id int
);

ALTER TABLE vibration_sensor_entity
    ADD CONSTRAINT htrefgdst
        FOREIGN KEY (room_id)
            REFERENCES room (id);

ALTER TABLE vibration_sensor
    ADD CONSTRAINT htrdsgvfn
        FOREIGN KEY (vibration_sensor_entity_id)
            REFERENCES vibration_sensor_entity(id);


create table contact_sensor(
    id int primary key,
    description varchar(255),
    sensor_identification varchar(255),
    time_stamp varchar(255),
    temperature float,
    contact boolean,
    is_on_connection boolean,
    is_on_contact_sensor_entity boolean,
    contact_sensor_entity_id int,
    connection_id int

);

create table contact_sensor_entity(
    id int primary key,
    description varchar(255),
    contact_sensor_entity_identification varchar(255),
    room_id int,
    contact_sensor_entity_id int
);

ALTER TABLE contact_sensor_entity
    ADD CONSTRAINT fsadjztn
        FOREIGN KEY (room_id)
            REFERENCES room(id);

ALTER TABLE contact_sensor
    ADD CONSTRAINT fsadjztnfds
        FOREIGN KEY (contact_sensor_entity_id)
            REFERENCES contact_sensor_entity(id);

ALTER TABLE contact_sensor
    ADD CONSTRAINT afsadjztnfds
        FOREIGN KEY (connection_id)
            REFERENCES connection(id);


create table electric_sensor(
    id int primary key,
    sensor_identification varchar(255),
    description varchar(255),
    time_stamp varchar(255),
    power float,
    voltage float,
    energy float,
    curent float,
    room_id int
);

ALTER TABLE electric_sensor
    ADD CONSTRAINT afsfadjztnfds
        FOREIGN KEY (room_id)
            REFERENCES room(id);


create table electric_sensor_entity(
    id int primary key,
    description varchar(255),
    electric_sensor_entity_identification varchar(255)
);