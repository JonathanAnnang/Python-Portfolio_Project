CREATE DATABASE SurveyDB;
GO

USE SurveyDB;

CREATE TABLE survey_responses (
    id INT IDENTITY(1,1) PRIMARY KEY,
    respondent_id VARCHAR(50),
    country VARCHAR(50),
    age INT,
    gender VARCHAR(10),
    income FLOAT,
    response_score INT,
    age_group VARCHAR(20),
    income_band VARCHAR(20),
    created_at DATETIME DEFAULT GETDATE()
);

CREATE TABLE countries (
    country_code VARCHAR(10) PRIMARY KEY,
    country_name VARCHAR(50)
);