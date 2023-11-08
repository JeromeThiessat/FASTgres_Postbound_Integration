
from models.context import Context
from query_encoding.query import Query
from query_encoding.feature_extractor import EncodingInformation
from query_encoding.query_encoder import Encoder


class EncodedQuery:

    def __init__(self, context: Context, query: Query, encoding_information: EncodingInformation):
        self.context = context
        self.query = query
        self.enc_info = encoding_information
        self.encoded_query = None

    def encode_query(self):
        encoder = Encoder(self.context, self.enc_info)
        self.encoded_query = encoder.encode_query(encoder.build_feature_dict_old(self.query))