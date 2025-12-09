from pydantic import BaseModel


class News(BaseModel):
    """
    Represents the data structure of a News.
    """

    title: str
    description: str
    publishtime: str
    url: str
    provider: str

class DetailedNews(BaseModel):
    """
    Represents the data structure of a News.
    """

    Title: str
    shortdescription: str
    detaileddescription: str
    datetime: str
    author: str 