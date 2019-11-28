import pandas as pd
from mamba import description, context, it
from expects import expect, equal, contain_exactly, raise_error
from blsqpy.dhis2_client import Dhis2Client
import responses


@responses.activate
def test_for_geometry_type(geometry_type, expected_types):
    client = Dhis2Client("https://admin:district@play.dhis2.org/2.30")
    geo_df = client.get_geodataframe(geometry_type=geometry_type)

    def as_type(x):
        return x['geometry'].__class__.__name__

    geo_df['geometry_type'] = geo_df.apply((lambda x: as_type(x)), axis=1)

    geometry_types = geo_df['geometry_type'].unique()

    expect(geometry_types.tolist()).to(contain_exactly(*expected_types))


def fixture_with_point():
    return {

        "level": 4,
        "name": "Ahamadyya Mission Cl",
        "id": "plnHVbJR6p4",
        "path": "/ImspTQPwCqd/PMa2VCrupOd/QywkxFudXrC/plnHVbJR6p4",
        "coordinates": "[-12.9487,9.0131]"
    }


def fixture_with_multipolygones():
    return {

        "level": 3,
        "name": "Badjia",
        "id": "YuQRtpLP10I",
        "path": "/ImspTQPwCqd/O6uvpzGd5pu/YuQRtpLP10I",
        "coordinates": "[[[[-11.3516,8.0819],[-11.3553,8.0796],[-11.3592,8.0779],[-11.3615,8.0764],[-11.3665,8.0724],[-11.374,8.0686],[-11.3765,8.0678],[-11.3815,8.0666],[-11.3859,8.0644],[-11.3891,8.0631],[-11.3934,8.0607],[-11.3972,8.0589],[-11.3994,8.0572],[-11.4048,8.0523],[-11.4075,8.0501],[-11.4115,8.0482],[-11.4144,8.0461],[-11.4169,8.0434],[-11.4184,8.0406],[-11.4189,8.0384],[-11.4192,8.0331],[-11.42,8.0298],[-11.4236,8.024],[-11.4258,8.0228],[-11.4339,8.0207],[-11.4389,8.0222],[-11.4417,8.0235],[-11.4428,8.0253],[-11.4461,8.0281],[-11.448,8.0307],[-11.449,8.0344],[-11.4492,8.0454],[-11.4494,8.0494],[-11.4501,8.0522],[-11.4521,8.0567],[-11.4528,8.0595],[-11.4531,8.0625],[-11.4532,8.0677],[-11.4527,8.0727],[-11.4514,8.0786],[-11.4527,8.0836],[-11.4532,8.0874],[-11.4535,8.0964],[-11.4535,8.1033],[-11.4529,8.1072],[-11.4507,8.1126],[-11.45,8.1163],[-11.4502,8.1202],[-11.4509,8.1229],[-11.4532,8.1282],[-11.4534,8.1325],[-11.4524,8.1357],[-11.4482,8.1423],[-11.4442,8.1401],[-11.442,8.14],[-11.44,8.1409],[-11.4369,8.1434],[-11.434,8.145],[-11.4298,8.1459],[-11.4273,8.1468],[-11.4206,8.1501],[-11.4166,8.1532],[-11.4096,8.1603],[-11.4056,8.1634],[-11.4026,8.1641],[-11.3982,8.1636],[-11.3938,8.1616],[-11.3913,8.1609],[-11.3859,8.1603],[-11.3833,8.1597],[-11.3779,8.1574],[-11.3736,8.1564],[-11.3695,8.1524],[-11.3657,8.1475],[-11.3638,8.1444],[-11.3623,8.1407],[-11.3589,8.1286],[-11.3574,8.1218],[-11.3525,8.1071],[-11.3509,8.1012],[-11.3505,8.0968],[-11.3504,8.091],[-11.3508,8.0868],[-11.3516,8.0819]]]]"

    }


def fixture_without_coordinates():
    return {

        "level": 4,
        "name": "Ahamadyya Mission Cl",
        "id": "plnHVbJR6p4",
        "path": "/ImspTQPwCqd/PMa2VCrupOd/QywkxFudXrC/plnHVbJR6p4",
        "coordinates": None

    }


with description('Dhis2Client#get_geodataframe') as self:
    with it('initialize with an url and return a geodataframe with points'):
        responses.add(responses.GET, 'https://admin:district@play.dhis2.org/2.30/api/organisationUnits?fields=id%2Cname%2Ccoordinates%2Cgeometry%2Clevel%2Cpath&paging=false&filter=featureType%3Aeq%3APOINT',
                      json={
                          'organisationUnits': [
                              fixture_with_point()
                          ]
                      }, status=200)
        test_for_geometry_type("point", ["Point"])

    with it('initialize with an url and return a geodataframe with poly and multipolygons'):
        responses.add(responses.GET, 'https://admin:district@play.dhis2.org/2.30/api/organisationUnits?fields=id%2Cname%2Ccoordinates%2Cgeometry%2Clevel%2Cpath&paging=false&filter=featureType%3Ain%3A%5BPOLYGON%2CMULTI_POLYGON%5D',
                      json={
                          'organisationUnits': [
                              fixture_with_multipolygones()
                          ]
                      }, status=200)

        test_for_geometry_type("shape", ["MultiPolygon"])

    with it('initialize with an url and return a geodataframe with points, poly, multipoly and without coordinates'):
        responses.add(responses.GET, 'https://admin:district@play.dhis2.org/2.30/api/organisationUnits?fields=id%2Cname%2Ccoordinates%2Cgeometry%2Clevel%2Cpath&paging=false',
                      json={
                          'organisationUnits': [
                              fixture_without_coordinates(),
                              fixture_with_point(),
                              fixture_with_multipolygones(),
                          ]
                      }, status=200)

        test_for_geometry_type(None, ['NoneType', 'Point', 'MultiPolygon'])

    with it('fails when wrong geometry type'):
        expect(lambda:  test_for_geometry_type("badddd", None)).to(
            raise_error(Exception,
                        "unsupported geometry type 'badddd'? Should be point,shape or None")
        )


@responses.activate
def test_for_organisation_units_structure():
    client = Dhis2Client("https://admin:district@play.dhis2.org/2.30")
    return client.organisation_units_structure()


with description('Dhis2Client#organisation_units_structure') as self:
    with it('fetch these columns'):

        responses.add(responses.GET, "https://admin:district@play.dhis2.org/2.30/api/organisationUnits?fields=id%2Cname%2Cpath%2CcontactPerson%2CmemberCount%2CfeatureType%2Ccoordinates%2CclosedDate%2CphoneNumber%2CmemberCount&paging=false",
                      json={
                          'organisationUnits': [
                              {
                                  "name": "Agape CHP",
                                  "id": "GvFqTavdpGE",
                                  "path": "/ImspTQPwCqd/O6uvpzGd5pu/U6Kr7Gtpidn/GvFqTavdpGE",
                                  "featureType": "NONE"
                              },
                              {
                                  "name": "Ahamadyya Mission Cl",
                                  "id": "plnHVbJR6p4",
                                  "path": "/ImspTQPwCqd/PMa2VCrupOd/QywkxFudXrC/plnHVbJR6p4",
                                  "featureType": "POINT",
                                  "coordinates": "[-12.9487,9.0131]"
                              },
                              {
                                  "name": "Ahmadiyya Muslim Hospital",
                                  "id": "BV4IomHvri4",
                                  "path": "/ImspTQPwCqd/eIQbndfxQMb/NNE0YMCDZkO/BV4IomHvri4",
                                  "featureType": "POINT",
                                  "coordinates": "[-12.2231,8.466]"
                              },
                              {
                                  "name": "Air Port Centre, Lungi",
                                  "id": "qjboFI0irVu",
                                  "path": "/ImspTQPwCqd/TEQlaapDQoK/vn9KJsLyP5f/qjboFI0irVu",
                                  "featureType": "POINT",
                                  "coordinates": "[-13.2027,8.6154]"
                              },
                          ]
                      }, status=200)

        df = test_for_organisation_units_structure()
        print(df)


def test_for_data_element_structure():
    client = Dhis2Client("https://admin:district@play.dhis2.org/2.32.3")
    return client.data_element_structure()


with description('Dhis2Client#data_element_structure') as self:
    with it('fetch these columns'):

        responses.add(responses.GET, "https://play.dhis2.org/2.32.3/api/dataElements.json?fields=id,name,shortName,valueType,domainType,code,aggregationType,categoryCombo[id,name,categoryOptionCombos[id,name]]&paging=false",
                      json={
                          'organisationUnits': [
                              {
                                  "code": "DE_1148614",
                                  "name": "Accute Flaccid Paralysis (Deaths < 5 yrs)",
                                  "id": "FTRrcoaog83",
                                  "shortName": "Accute Flaccid Paral (Deaths < 5 yrs)",
                                  "aggregationType": "SUM",
                                  "domainType": "AGGREGATE",
                                  "valueType": "NUMBER",
                                  "categoryCombo": {
                                      "name": "default",
                                      "id": "bjDvmb4bfuf",
                                      "categoryOptionCombos": [
                                          {
                                              "name": "default",
                                              "id": "HllvX50cXC0"
                                          }
                                      ]
                                  }
                              },
                           {
                                  "code": "DE_359049",
                                  "name": "Acute Flaccid Paralysis (AFP) follow-up",
                                  "id": "P3jJH5Tu5VC",
                                  "shortName": "AFP follow-up",
                                  "aggregationType": "SUM",
                                  "domainType": "AGGREGATE",
                                  "valueType": "NUMBER",
                                  "categoryCombo": {
                                      "name": "Morbidity Cases",
                                      "id": "t3aNCvHsoSn",
                                      "categoryOptionCombos": [
                                          {
                                              "name": "12-59m",
                                              "id": "wHBMVthqIX4"
                                          },
                                          {
                                              "name": "5-14y",
                                              "id": "SdOUI2yT46H"
                                          },
                                          {
                                              "name": "0-11m",
                                              "id": "S34ULMcHMca"
                                          },
                                          {
                                              "name": "15y+",
                                              "id": "jOkIbJVhECg"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "code": "DE_388",
                                  "name": "Acute Flaccid Paralysis (AFP) new",
                                  "id": "FQ2o8UBlcrS",
                                  "shortName": "AFP new",
                                  "aggregationType": "SUM",
                                  "domainType": "AGGREGATE",
                                  "valueType": "NUMBER",
                                  "categoryCombo": {
                                      "name": "Morbidity Cases",
                                      "id": "t3aNCvHsoSn",
                                      "categoryOptionCombos": [
                                          {
                                              "name": "12-59m",
                                              "id": "wHBMVthqIX4"
                                          },
                                          {
                                              "name": "5-14y",
                                              "id": "SdOUI2yT46H"
                                          },
                                          {
                                              "name": "0-11m",
                                              "id": "S34ULMcHMca"
                                          },
                                          {
                                              "name": "15y+",
                                              "id": "jOkIbJVhECg"
                                          }
                                      ]
                                  }
                              },
                              {
                                  "code": "DE_358943",
                                  "name": "Acute Flaccid Paralysis (AFP) referrals",
                                  "id": "M62VHgYT2n0",
                                  "shortName": "AFP referrals",
                                  "aggregationType": "SUM",
                                  "domainType": "AGGREGATE",
                                  "valueType": "NUMBER",
                                  "categoryCombo": {
                                      "name": "Morbidity Referrals",
                                      "id": "ck7mRNwGDjP",
                                      "categoryOptionCombos": [
                                          {
                                              "name": "0-4y",
                                              "id": "o2gxEt6Ek2C"
                                          },
                                          {
                                              "name": "5y+",
                                              "id": "ba1FkzknqS8"
                                          }
                                      ]
                                  }
                              },
                          ]
                      }, status=200)

        df = test_for_data_element_structure()
        print(df)
