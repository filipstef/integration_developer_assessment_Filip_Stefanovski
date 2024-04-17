from abc import ABC, abstractmethod
import inspect
import sys
import json
from collections import namedtuple
from datetime import timedelta, datetime
from json import JSONDecodeError
from typing import Optional
import uuid
from django.utils import timezone

from hotel.exceptions import JsonValueError, IncorrectHotelIdException
from hotel.external_api import (
    get_reservations_for_given_checkin_date,
    get_reservation_details,
    get_guest_details,
    APIError,
)

from hotel.models import Stay, Guest, Hotel, Language

now = datetime.now()

class PMS(ABC):
    """
    Abstract class for Property Management Systems.
    """

    def __init__(self):
        pass

    @property
    def name(self):
        longname = self.__class__.__name__
        return longname[4:]

    @abstractmethod
    def clean_webhook_payload(self, payload: str) -> dict:
        """
        Clean the json payload and return a usable object.
        Make sure the payload contains all the needed information to handle it properly
        """
        raise NotImplementedError

    @abstractmethod
    def handle_webhook(self, webhook_data: dict) -> bool:
        """
        This method is called when we receive a webhook from the PMS.
        Handle webhook handles the events and updates relevant models in the database.
        Requirements:
            - Now that the PMS has notified you about an update of a reservation, you need to
                get more details of this reservation. For this, you can use the mock API
                call get_reservation_details(reservation_id).
            - Handle the payload for the correct hotel.
            - Update or create a Stay.
            - Update or create Guest details.
        """
        raise NotImplementedError

    @abstractmethod
    def update_tomorrows_stays(self) -> bool:
        """
        This method is called every day at 00:00 to update the stays with a checkin date tomorrow.
        Requirements:
            - Get all stays checking in tomorrow by calling the mock API endpoint get_reservations_for_given_checkin_date.
            - Update or create the Stays.
            - Update or create Guest details. Deal with missing and incomplete data yourself
                as you see fit. Deal with the Language yourself. country != language.
        """
        raise NotImplementedError

    @abstractmethod
    def stay_has_breakfast(self, stay: Stay) -> Optional[bool]:
        """
        This method is called when we want to know if the stay includes breakfast.
        Notice that the breakfast data is not stored in any of the models, we always want real time data.
        - Return True if the stay includes breakfast, otherwise False. Return None if you don't know.
        """
        raise NotImplementedError


class PMS_Mews(PMS):
    def clean_webhook_payload(self, payload: str) -> dict:
        if not payload:
            raise JsonValueError("Incorrect json input")
        try:
            webhook_data = json.loads(payload, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except JSONDecodeError:
            raise JSONDecodeError

        return webhook_data

    def handle_webhook(self, webhook_data: dict) -> bool:
        for event in webhook_data.Events:
            try:
                stay = make_api_call_with_retry(self.name, get_reservation_details, event.Value.ReservationId)
                guest = make_api_call_with_retry(self.name, get_guest_details, stay.GuestId)

                guest_id = update_or_create_guest(guest)
                update_or_create_stay(stay, guest_id)

            except IncorrectHotelIdException:
                continue
            except ValueError:
                continue
            except Exception:
                return False

        return True
    """
    import hotel.pms_systems
    pms_instance = hotel.pms_systems.PMS_Mews()
    pms_instance.update_tomorrows_stays()
    """
    def update_tomorrows_stays(self) -> bool:
        tomorrow_date = (datetime.now() + timedelta(days=1)).date()
        stays = make_api_call_with_retry(self.name, get_reservations_for_given_checkin_date, str(tomorrow_date))

        for stay in stays:
            try:
                guest = make_api_call_with_retry(self.name, get_guest_details, stay.GuestId)

                guest_id = update_or_create_guest(guest)
                update_or_create_stay(stay, guest_id)
            except IncorrectHotelIdException:
                continue
            except ValueError:
                continue
            except Exception:
                return False

        return True

    def stay_has_breakfast(self, stay: Stay) -> Optional[bool]:
        stay_to_check = make_api_call_with_retry(self.name, get_reservation_details, stay.ReservationId)

        return stay_to_check.BreakfastIncluded

def get_pms(name):
    fullname = "PMS_" + name.capitalize()
    # find all class names in this module
    # from https://stackoverflow.com/questions/1796180/
    current_module = sys.modules[__name__]
    clsnames = [x[0] for x in inspect.getmembers(current_module, inspect.isclass)]

    # if we have a PMS class for the given name, return an instance of it
    return getattr(current_module, fullname)() if fullname in clsnames else False

def update_or_create_guest(guest: Guest) -> int or None:
    language = get_guest_language(guest.Country)
    if not guest.Phone or guest.Phone == 'Not available':
        guest_id = None
    else:
        guest_from_db = Guest.objects.filter(phone=guest.Phone)
        if guest_from_db.exists():
            guest_from_db.update(name=(uuid.uuid4() if not guest.Name else guest.Name), phone=guest.Phone, language = language,
                                 updated_at=now)
            guest_id = Guest.objects.get(phone=guest.Phone).id
        else:
            new_guest = Guest(name=(uuid.uuid4() if not guest.Name else guest.Name), phone=guest.Phone, language = language, created_at=now)
            new_guest.save()
            guest_id = (None if not Guest.objects.filter(phone=guest.Phone).exists() else Guest.objects.get(
                phone=guest.Phone).id)
    return guest_id

def update_or_create_stay(stay: Stay, guest_id):
    hotel_id = Hotel.objects.all().get(pms_hotel_id=stay.HotelId).id

    if not hotel_id:
        raise IncorrectHotelIdException("Hotel ID not found")

    stay_from_db = Stay.objects.filter(pms_reservation_id=stay.ReservationId)
    if stay_from_db.exists():
        stay_from_db.update(hotel_id=hotel_id, pms_reservation_id=stay.ReservationId, pms_guest_id=stay.GuestId,
                            guest_id=guest_id, status=stay.Status, checkin=stay.CheckInDate, checkout=stay.CheckOutDate, updated_at=now)
    else:
        new_stay = Stay(hotel_id=hotel_id, pms_reservation_id=stay.ReservationId, pms_guest_id=stay.GuestId,
                 guest_id=guest_id, status=stay.Status, checkin=stay.CheckInDate, checkout=stay.CheckOutDate, created_at=now)
        new_stay.save()

def get_guest_language(country) -> str:
    if not country:
        return "None"

    for choice in Language.choices:
        if choice[0] == country.lower():
            return choice[1]

    return "None"

#Ensure that even when the api is not available we retry a few more times to get the data
def make_api_call_with_retry(pms_name, request, parameters) -> dict:
    data = None
    pms = get_pms(pms_name)
    retry_times = 0

    while retry_times < 10:
        try:
            data = request(parameters)
            data = pms.clean_webhook_payload(data)
            break
        except APIError as api_error:
            print(str(api_error))
            retry_times += 1

    if retry_times == 10:
        raise APIError

    return data
