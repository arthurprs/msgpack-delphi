{*
 *				Delphi MsgPack
 *
 * Usage allowed under the restrictions of the Lesser GNU General Public License
 * or alternatively the restrictions of the Mozilla Public License 1.1
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
 * the specific language governing rights and limitations under the License.
 *
 * Embarcadero Technologies Inc is not permitted to use or redistribute
 * this source code without explicit permission.
 *
 * Unit owner : Arthur Pires Ribeiro Silva <arthurprs@gmail.com>
 * Web site   : https://github.com/arthurprs/msgpack-delphi
 *
 * Inspiration: http://code.google.com/p/superobject/
 *}
 
unit msgpack;

interface

uses
  SysUtils, Classes;

{$IF defined(FPC) or defined(VER170) or defined(VER180) or defined(VER190) or defined(VER200) or defined(VER210) or defined(VER220) or defined(VER230)}
{$DEFINE HAVE_INLINE}
{$IFEND}

type
{$IFDEF UNICODE}
  NativeString = UnicodeString;
{$ELSE}
  NativeString = AnsiString;
  RawByteString = AnsiString;
  UnicodeString = WideString;
{$ENDIF}
  IMsgPackObject = interface;
  TMsgPackObject = class;

  TMsgPackType = (mptNil = 0, mptBoolean, mptInteger, mptFloat, mptDouble, mptRaw, mptArray, mptMap);

  TMsgPackArray = class
  private
    FItems: TList;
    FCount: Integer;
  public
    constructor Create();
    destructor Destroy; override;
    procedure Clear();
    function Add(const Item: IMsgPackObject): Integer;
    function Get(const Index: Integer): IMsgPackObject;
    procedure Delete(const Index: Integer);
    procedure Put(const Index: Integer; const Value: IMsgPackObject);
    property Items[const index: Integer]: IMsgPackObject read Get write Put; default;
    property Count: Integer read FCount;
  end;

  TMsgPackMapIterator = record
    FCursor: Integer;
    Key: NativeString;
    Value: IMsgPackObject;
  end;

  TMsgPackMapBucket = record
    Key: NativeString;
    Value: IMsgPackObject;
  end;

  PMsgPackMapBucket = ^TMsgPackMapBucket;
  TMsgPackMapBuckets = array of TMsgPackMapBucket;

  TMsgPackMap = class
  private
    FCount: Integer;
    FCapacity: Integer;
    FBuckets: TMsgPackMapBuckets;
    // double the hashtable size
    procedure Grow();
    // Return the bucket containing the key or the bucket where it could be inserted
    function FindBucket(const Key: NativeString): PMsgPackMapBucket;
  public
    constructor Create();
    destructor Destroy; override;
    procedure Clear();
    procedure Put(const Key: NativeString; const Value: IMsgPackObject);
    function Get(const Key: NativeString): IMsgPackObject;
    procedure Delete(const Key: NativeString);
    procedure IteratorInit(var Iterator: TMsgPackMapIterator);
    function IteratorAdvance(var Iterator: TMsgPackMapIterator): Boolean;
    property Count: Integer read FCount;
    property Values[const Key: NativeString]: IMsgPackObject read Get write Put; default;
  end;

  IMsgPackObject = interface
    ['{FAC2A072-C1D1-46A3-BD2F-9F9050FD1E1F}']
    // type
    function GetObjectType: TMsgPackType;
    property ObjectType: TMsgPackType read GetObjectType;
    // conversions
    function AsNil: Boolean;
    function AsBoolean: Boolean;
    function AsInteger: Int64;
    function AsDouble: Double;
    function AsFloat: Single;
    function AsBytes: RawByteString;
    function AsString: UnicodeString;
    function AsArray: TMsgPackArray;
    function AsMap: TMsgPackMap;
    // clone
    function Clone(): IMsgPackObject;
    // dump
    function AsMsgPack(): RawByteString;
    procedure Write(const Stream: TStream);
    // parse
    procedure Read(const Stream: TStream);
  end;

  TMsgPackVariant = record
    case TMsgPackType of
      mptBoolean:
      (dataBoolean: Boolean);
      mptInteger:
      (dataInteger: Int64);
      mptFloat:
      (dataFloat: Single);
      mptDouble:
      (dataDouble: Double);
      mptArray:
      (dataArray: TMsgPackArray);
      mptMap:
      (dataMap: TMsgPackMap);
  end;

  TMsgPackObject = class(TInterfacedObject, IMsgPackObject)
  private
    FType: TMsgPackType;
    FVariant: TMsgPackVariant;
    FBytes: RawByteString;
  public
    destructor Destroy; override;
    // creating new objects
    constructor Create(); overload; // create as nil
    constructor Create(const Value: Boolean); overload;
    constructor Create(const Value: Int64); overload;
    constructor Create(const Value: Single); overload;
    constructor Create(const Value: Double); overload;
    constructor Create(const Value: RawByteString); overload;
    constructor Create(const Value: UnicodeString); overload;
    constructor Create(const ObjectType: TMsgPackType); overload;
    // parsing
    constructor Parse(const Str: RawByteString); overload;
    constructor Parse(const Stream: TStream); overload;
    // type
    function GetObjectType: TMsgPackType;
    property ObjectType: TMsgPackType read GetObjectType;
    // casts
    function AsNil: Boolean;
    function AsBoolean: Boolean;
    function AsInteger: Int64;
    function AsDouble: Double;
    function AsFloat: Single;
    function AsBytes: RawByteString;
    function AsString: UnicodeString;
    function AsArray: TMsgPackArray;
    function AsMap: TMsgPackMap;
    // clone
    function Clone(): IMsgPackObject;
    // dump
    function AsMsgPack(): RawByteString;
    procedure Write(const Stream: TStream);
    // parse
    procedure Read(const Stream: TStream);
  end;

function MPO(const Value: Boolean): IMsgPackObject; overload; {$IFDEF HAVE_INLINE}inline; {$ENDIF}
function MPO(const Value: Int64): IMsgPackObject; overload; {$IFDEF HAVE_INLINE}inline; {$ENDIF}
function MPO(const Value: Single): IMsgPackObject; overload; {$IFDEF HAVE_INLINE}inline; {$ENDIF}
function MPO(const Value: Double): IMsgPackObject; overload; {$IFDEF HAVE_INLINE}inline; {$ENDIF}
function MPO(const Value: RawByteString): IMsgPackObject; overload; {$IFDEF HAVE_INLINE}inline; {$ENDIF}
function MPO(const Value: UnicodeString): IMsgPackObject; overload; {$IFDEF HAVE_INLINE}inline; {$ENDIF}
function MPO(const ObjectType: TMsgPackType): IMsgPackObject; overload; {$IFDEF HAVE_INLINE}inline; {$ENDIF}
function MPOFromMsgPack(const Str: RawByteString): IMsgPackObject; {$IFDEF HAVE_INLINE}inline; {$ENDIF}

implementation

function MPOFromMsgPack(const Str: RawByteString): IMsgPackObject;
begin
  Result := TMsgPackObject.Parse(Str);
end;

function MPO(const Value: Boolean): IMsgPackObject;
begin
  Result := TMsgPackObject.Create(Value);
end;

function MPO(const Value: Int64): IMsgPackObject;
begin
  Result := TMsgPackObject.Create(Value);
end;

function MPO(const Value: Single): IMsgPackObject;
begin
  Result := TMsgPackObject.Create(Value);
end;

function MPO(const Value: Double): IMsgPackObject;
begin
  Result := TMsgPackObject.Create(Value);
end;

function MPO(const Value: RawByteString): IMsgPackObject;
begin
  Result := TMsgPackObject.Create(Value);
end;

function MPO(const Value: UnicodeString): IMsgPackObject;
begin
  Result := TMsgPackObject.Create(Value);
end;

function MPO(const ObjectType: TMsgPackType): IMsgPackObject;
begin
  Result := TMsgPackObject.Create(ObjectType);
end;

{ TMsgPackMap }

var
  DeletedBucket: IMsgPackObject;

function HashString(const Key: NativeString): Cardinal;
var
  i: Integer;
begin
  Result := 0;
  for i := 1 to Length(Key) do
    Result := (Result xor Ord(Key[i])) * 33;
end;

procedure TMsgPackMap.Put(const Key: NativeString; const Value: IMsgPackObject);
var
  bucket: PMsgPackMapBucket;
begin
  if FCount >= FCapacity div 2 then
    Grow();

  bucket := FindBucket(Key);
  if bucket.Key = '' then
  begin
    // new entry
    Inc(FCount);
    bucket.Key := Key;
    bucket.Value := Value;
  end
  else
  begin
    // replace
    bucket.Value := Value;
  end;
end;

procedure TMsgPackMap.Clear;
begin
  SetLength(FBuckets, 0);
  FCount := 0;
end;

constructor TMsgPackMap.Create;
begin

end;

procedure TMsgPackMap.Delete(const Key: NativeString);
var
  bucket: PMsgPackMapBucket;
begin
  bucket := FindBucket(Key);
  if bucket.Key <> '' then
  begin
    bucket.Key := '';
    bucket.Value := DeletedBucket;
    Dec(FCount);
  end;
end;

destructor TMsgPackMap.Destroy;
begin

end;

function TMsgPackMap.Get(const Key: NativeString): IMsgPackObject;
var
  bucket: PMsgPackMapBucket;
begin
  Result := nil;
  if FCount = 0 then
    Exit;
  if Key = '' then
    raise EInvalidOperation.Create('Empty Key');
  bucket := FindBucket(Key);
  if (bucket.Value <> nil) and (bucket.Value <> DeletedBucket) then
    Result := bucket.Value;
end;

procedure TMsgPackMap.IteratorInit(var Iterator: TMsgPackMapIterator);
begin
  Iterator.FCursor := -1;
end;

function TMsgPackMap.IteratorAdvance(var Iterator: TMsgPackMapIterator): Boolean;
begin
  Result := False;
  Inc(Iterator.FCursor);
  while Iterator.FCursor < FCapacity do
  begin
    if FBuckets[Iterator.FCursor].Key <> '' then
    begin
      Iterator.Key := FBuckets[Iterator.FCursor].Key;
      Iterator.Value := FBuckets[Iterator.FCursor].Value;
      Result := True;
      Exit;
    end;
    Inc(Iterator.FCursor);
  end;
end;

procedure TMsgPackMap.Grow();
var
  oldCapacity: Integer;
  oldBuckets: TMsgPackMapBuckets;
  i: Integer;
begin
  oldCapacity := FCapacity;
  oldBuckets := FBuckets;

  if oldCapacity = 0 then
    FCapacity := 32
  else
    FCapacity := oldCapacity * 2;

  SetLength(FBuckets, 0);
  SetLength(FBuckets, FCapacity);
  FCount := 0; // reset

  for i := 0 to oldCapacity - 1 do
  begin
    if oldBuckets[i].Key <> '' then
      Put(oldBuckets[i].Key, oldBuckets[i].Value);
  end;
end;

function TMsgPackMap.FindBucket(const Key: NativeString): PMsgPackMapBucket;
var
  hash: Cardinal;
begin
  hash := HashString(Key) and (FCapacity - 1); // apply mask
  Result := @FBuckets[hash];
  while FBuckets[hash].Value <> nil do
  begin
    if FBuckets[hash].Key = Key then
      Exit;
    hash := (hash + 1) and (FCapacity - 1);
    Result := @FBuckets[hash];
  end;
end;

{ TMsgPackArray }

function TMsgPackArray.Add(const Item: IMsgPackObject): Integer;
begin
  Inc(FCount);
  Item._AddRef();
  Result := FItems.Add(Pointer(Item));
end;

procedure TMsgPackArray.Clear;
var
  i: Integer;
begin
  for i := 0 to FItems.Count - 1 do
    IMsgPackObject(Pointer(FItems[i]))._Release();
  FItems.Clear();
  FCount := 0;
end;

constructor TMsgPackArray.Create;
begin
  FItems := TList.Create;
end;

procedure TMsgPackArray.Delete(const Index: Integer);
begin
  IMsgPackObject(Pointer(FItems[Index]))._Release();
  FItems.Delete(Index);
  Dec(FCount);
end;

destructor TMsgPackArray.Destroy;
begin
  Clear();
  FItems.Free;
  inherited;
end;

function TMsgPackArray.Get(const Index: Integer): IMsgPackObject;
begin
  Result := IMsgPackObject(Pointer(FItems[Index]))
end;

procedure TMsgPackArray.Put(const Index: Integer; const Value: IMsgPackObject);
begin
  IMsgPackObject(Pointer(FItems[Index]))._AddRef();
  FItems.Add(Pointer(FItems[Index]));
end;

{ TMsgPackObject }

function TMsgPackObject.AsArray: TMsgPackArray;
begin
  case FType of
    mptNil:
      Result := nil;
    mptArray:
      Result := FVariant.dataArray;
  else
    raise EInvalidCast.Create('Invalid cast');
  end;
end;

function TMsgPackObject.AsBoolean: Boolean;
begin
  case FType of
    mptNil:
      Result := False;
    mptBoolean:
      Result := FVariant.dataBoolean;
    mptInteger:
      Result := FVariant.dataInteger <> 0;
    mptFloat:
      Result := FVariant.dataFloat <> 0.0;
    mptDouble:
      Result := FVariant.dataDouble <> 0.0;
    mptArray:
      Result := FVariant.dataArray.Count <> 0;
    mptMap:
      Result := FVariant.dataMap.Count <> 0;
  else
    raise EInvalidCast.Create('Invalid cast');
  end;
end;

function TMsgPackObject.AsBytes: RawByteString;
begin
  case FType of
    mptNil:
      Result := '';
    mptRaw:
      Result := FBytes;
  else
    raise EInvalidCast.Create('Invalid cast');
  end;
end;

function TMsgPackObject.AsDouble: Double;
begin
  case FType of
    mptNil:
      Result := 0;
    mptInteger:
      Result := FVariant.dataInteger;
    mptFloat:
      Result := FVariant.dataFloat;
    mptDouble:
      Result := FVariant.dataDouble;
  else
    raise EInvalidCast.Create('Invalid cast');
  end;
end;

function TMsgPackObject.AsFloat: Single;
begin
  case FType of
    mptNil:
      Result := 0;
    mptInteger:
      Result := FVariant.dataInteger;
    mptFloat:
      Result := FVariant.dataFloat;
    mptDouble:
      Result := FVariant.dataDouble;
  else
    raise EInvalidCast.Create('Invalid cast');
  end;
end;

function TMsgPackObject.AsInteger: Int64;
begin
  case FType of
    mptNil:
      Result := 0;
    mptBoolean:
      if FVariant.dataBoolean then
        Result := 1
      else
        Result := 0;
    mptInteger:
      Result := FVariant.dataInteger;
    mptFloat:
      Result := Trunc(FVariant.dataFloat);
    mptDouble:
      Result := Trunc(FVariant.dataDouble);
  else
    raise EInvalidCast.Create('Invalid cast');
  end;
end;

function TMsgPackObject.AsNil: Boolean;
begin
  case FType of
    mptNil:
      Result := True;
    mptBoolean:
      Result := FVariant.dataBoolean = False;
    mptInteger:
      Result := FVariant.dataInteger = 0;
    mptArray:
      Result := FVariant.dataArray.Count = 0;
    mptMap:
      Result := FVariant.dataMap.Count = 0;
  else
    raise EInvalidCast.Create('Invalid cast');
  end;
end;

function TMsgPackObject.AsString: UnicodeString;
begin
{$IFDEF UNICODE}
  Result := UTF8ToUnicodeString(AsBytes());
{$ELSE}
  Result := UTF8Decode(AsBytes());
{$ENDIF}
end;

function TMsgPackObject.AsMap: TMsgPackMap;
begin
  case FType of
    mptNil:
      Result := nil;
    mptMap:
      Result := FVariant.dataMap;
  else
    raise EInvalidCast.Create('Invalid cast');
  end;
end;

constructor TMsgPackObject.Create;
begin
  FType := mptNil;
end;

constructor TMsgPackObject.Create(const Value: Single);
begin
  FType := mptFloat;
  FVariant.dataFloat := Value;
end;

constructor TMsgPackObject.Create(const Value: Int64);
begin
  FType := mptInteger;
  FVariant.dataInteger := Value;
end;

constructor TMsgPackObject.Create(const Value: Boolean);
begin
  FType := mptBoolean;
  FVariant.dataBoolean := Value;
end;

constructor TMsgPackObject.Create(const Value: Double);
begin
  FType := mptDouble;
  FVariant.dataDouble := Value;
end;

constructor TMsgPackObject.Create(const Value: RawByteString);
begin
  FType := mptRaw;
  FBytes := Value;
end;

constructor TMsgPackObject.Create(const ObjectType: TMsgPackType);
begin
  FType := ObjectType;
  case FType of
    mptArray:
      FVariant.dataArray := TMsgPackArray.Create;
    mptMap:
      FVariant.dataMap := TMsgPackMap.Create;
  end;
end;

constructor TMsgPackObject.Create(const Value: UnicodeString);
begin
  FType := mptRaw;
  FBytes := UTF8Encode(Value);
end;

destructor TMsgPackObject.Destroy;
begin
  case FType of
    mptArray:
      FVariant.dataArray.Free;
    mptMap:
      FVariant.dataMap.Free;
  end;
  inherited;
end;

function TMsgPackObject.AsMsgPack: RawByteString;
var
  resultStream: TStringStream;
begin
  resultStream := TStringStream.Create('');
  try
    Write(resultStream);
    Result := RawByteString(resultStream.DataString);
  finally
    resultStream.Free;
  end;
end;

procedure TMsgPackObject.Write(const Stream: TStream);

  procedure WriteByte(Value: Byte);
  begin
    Stream.Write(Value, 1);
  end;

  procedure WriteBEWord(Value: Word);
  begin
    Value := ((Value and $00FF) shl 8) or ((Value and $FF00) shr 8);
    Stream.Write(Value, 2);
  end;

  procedure WriteBEDWord(Value: Cardinal);
  begin
    Value := ((Value and $000000FF) shl 24) or ((Value and $0000FF00) shl 8) or ((Value and $00FF0000) shr 8) or
      ((Value and $FF000000) shr 24);
    Stream.Write(Value, 4);
  end;

  procedure WriteBEQWord(Value: Int64);
  var
    i: Integer;
    bytes: array[0..7] of Byte absolute Value;
  begin
    // definitely need some tunning..
    for i := 7 downto 0 do
      Stream.Write(bytes[i], 1);
  end;

  procedure WriteBytes(const Value: RawByteString);
  begin
    case Length(Value) of
      0..31:
        begin // fixraw
          WriteByte($A0 or Length(Value));
        end;
      32..High(Word):
        begin
          WriteByte($DA); // uint16
          WriteBEWord(Length(Value));
        end;
    else
      begin
        WriteByte($DB); // uint32
        WriteBEDWord(Length(Value));
      end;
    end;
    Stream.Write(Pointer(Value)^, Length(Value));
  end;

var
  i: Integer;
  mapIt: TMsgPackMapIterator;
begin
  case FType of
    mptNil:
      begin
        WriteByte($C0); // nil
      end;
    mptBoolean:
      begin
        if FVariant.dataBoolean then
          WriteByte($C3) // true
        else
          WriteByte($C2); // false
      end;
    mptInteger:
      begin
        if (FVariant.dataInteger >= 0) and (FVariant.dataInteger <= 127) then
        begin
          WriteByte(Byte(FVariant.dataInteger)); // pos fixnum
        end
        else if (FVariant.dataInteger >= -32) and (FVariant.dataInteger <= -1) then
        begin
          WriteByte($11100000 or Byte(FVariant.dataInteger)); // neg fixnum
        end
        else if (FVariant.dataInteger >= 127 + 1) and (FVariant.dataInteger <= High(Byte)) then
        begin
          WriteByte($CC); // uint8
          WriteByte(Byte(FVariant.dataInteger));
        end
        else if (FVariant.dataInteger >= High(Byte) + 1) and (FVariant.dataInteger <= High(Word)) then
        begin
          WriteByte($CD); // uint16
          WriteBEWord(Word(FVariant.dataInteger));
        end
        else if (FVariant.dataInteger >= High(Word) + 1) and (FVariant.dataInteger <= High(Cardinal)) then
        begin
          WriteByte($CE); // uint32
          WriteBEDWord(Cardinal(FVariant.dataInteger));
        end
        else if (FVariant.dataInteger >= Low(ShortInt)) and (FVariant.dataInteger <= -32 - 1) then
        begin
          WriteByte($D0); // int8
          WriteByte(ShortInt(FVariant.dataInteger));
        end
        else if (FVariant.dataInteger >= Low(SmallInt)) and (FVariant.dataInteger <= Low(ShortInt) - 1) then
        begin
          WriteByte($D1); // int16
          WriteBEWord(SmallInt(FVariant.dataInteger));
        end
        else if (FVariant.dataInteger >= Low(Integer)) and (FVariant.dataInteger <= Low(SmallInt) - 1) then
        begin
          WriteByte($D2); // int32
          WriteBEDWord(Integer(FVariant.dataInteger));
        end
        else
        begin
          WriteByte($D3); // int64
          WriteBEQWord(FVariant.dataInteger);
        end;
      end;
    mptFloat:
      begin
        WriteByte($CA); // float
        WriteBEDWord(PCardinal(@FVariant.dataFloat)^);
      end;
    mptDouble:
      begin
        WriteByte($CB); // double
        WriteBEQWord(PInt64(@FVariant.dataDouble)^);
      end;
    mptArray:
      begin
        // write the prefix
        case FVariant.dataArray.Count of
          0..15:
            begin
              WriteByte($90 or FVariant.dataArray.Count);
            end;
          16..High(Word):
            begin
              WriteByte($DC); // uint16
              WriteBEWord(FVariant.dataArray.Count);
            end;
        else
          begin
            WriteByte($DD); // uint32
            WriteBEDWord(FVariant.dataArray.Count);
          end;
        end;
        // write the items
        for i := 0 to FVariant.dataArray.Count - 1 do
          FVariant.dataArray.Get(i).Write(Stream);
      end;
    mptMap:
      begin
        // write the prefix
        case FVariant.dataMap.Count of
          0..15:
            begin
              WriteByte($80 or FVariant.dataMap.Count);
            end;
          16..High(Word):
            begin
              WriteByte($DE); // uint16
              WriteBEWord(FVariant.dataMap.Count);
            end;
        else
          begin
            WriteByte($DF); // uint32
            WriteBEDWord(FVariant.dataMap.Count);
          end;
        end;
        // write the pairs
        FVariant.dataMap.IteratorInit(mapIt);
        while FVariant.dataMap.IteratorAdvance(mapIt) do
        begin
{$IFDEF UNICODE}
          WriteBytes(UTF8Encode(mapIt.Key));
{$ELSE}
          WriteBytes(mapIt.Key);
{$ENDIF}
          mapIt.Value.Write(Stream);
        end;
      end;
    mptRaw:
      begin
        WriteBytes(FBytes);
      end;
  end;
end;

function TMsgPackObject.GetObjectType: TMsgPackType;
begin
  Result := FType;
end;

constructor TMsgPackObject.Parse(const Str: RawByteString);
var
  Stream: TStringStream;
begin
  Stream := TStringStream.Create(Str);
  try
    Read(Stream);
  finally
    Stream.Free;
  end;
end;

procedure TMsgPackObject.Read(const Stream: TStream);

  function ReadByte(): Byte;
  begin
    Stream.ReadBuffer(Result, 1);
  end;

  function ReadBEWord(): Word;
  begin
    Stream.ReadBuffer(Result, 2);
    Result := ((Result and $00FF) shl 8) or ((Result and $FF00) shr 8);
  end;

  function ReadBEDWord(): Cardinal;
  begin
    Stream.ReadBuffer(Result, 4);
    Result := ((Result and $000000FF) shl 24) or ((Result and $0000FF00) shl 8) or ((Result and $00FF0000) shr 8) or
      ((Result and $FF000000) shr 24);
  end;

  function ReadBEQWord(): Int64;
  var
    i: Integer;
    bytes: array[0..7] of Byte absolute Result;
  begin
    // definitely need some tunning..
    for i := 7 downto 0 do
      Stream.ReadBuffer(bytes[i], 1);
  end;

  function ReadBytes(Len: Integer = -1): RawByteString;
  var
    prefix: Byte;
    readLen: Integer;
  begin
    if Len = -1 then // need to get prefix?
    begin
      // read prefix
      prefix := ReadByte();
      if prefix and $A0 = $A0 then
        Len := prefix and (not $A0) // fixraw
      else if prefix = $DA then
        Len := ReadBEWord() // uint16
      else if prefix = $DB then
        Len := ReadBEDWord() // uint32
      else
        raise EParserError.CreateFmt('Expected a string in position %d', [Stream.Position - 1]);
    end;
    // read bytes
    SetLength(Result, Len);
    readLen := Stream.Read(Pointer(Result)^, Len);
    if readLen <> Len then
      raise EReadError.CreateFmt('Unexpected end of string expected %d bytes but got %d', [Len, readLen]);
  end;

  function ReadArray(const Count: Integer): TMsgPackArray;
  var
    i: Integer;
  begin
    Result := TMsgPackArray.Create;
    for i := 0 to Count - 1 do
    begin
      Result.Add(TMsgPackObject.Parse(Stream));
    end;
  end;

  function ReadMap(const Count: Integer): TMsgPackMap;
  var
    i: Integer;
    Key: NativeString;
    Value: IMsgPackObject;
  begin
    Result := TMsgPackMap.Create;
    for i := 0 to Count - 1 do
    begin
{$IFDEF UNICODE}
      Key := UTF8ToUnicodeString(ReadBytes());
{$ELSE}
      Key := ReadBytes();
{$ENDIF}
      Value := TMsgPackObject.Parse(Stream);
      Result.Put(Key, Value);
    end;
  end;

var
  prefix: Byte;
begin
  if FType <> mptNil then
    raise EInvalidOperation.Create('Can''t read to a non nil object');

  prefix := ReadByte();
  case prefix of
    $C0: // nil
      begin
        FType := mptNil;
      end;
    $C2, $C3: // boolean
      begin
        FType := mptBoolean;
        FVariant.dataBoolean := prefix = $C3;
      end;
    $CA: // float
      begin
        FType := mptFloat;
        FVariant.dataFloat := Single(Pointer(ReadBEDWord()));
      end;
    $CB: // double
      begin
        FType := mptDouble;
        FVariant.dataInteger := ReadBEQWord();
      end;
    $CC: // uint8
      begin
        FType := mptInteger;
        FVariant.dataInteger := ReadByte();
      end;
    $CD: // uint16
      begin
        FType := mptInteger;
        FVariant.dataInteger := Word(ReadBEWord());
      end;
    $CE: // uint32
      begin
        FType := mptInteger;
        FVariant.dataInteger := Cardinal(ReadBEDWord());
      end;
    $CF: // uint64
      begin
        FType := mptInteger;
        FVariant.dataInteger := ReadBEQWord();
      end;
    $D0: // int8
      begin
        FType := mptInteger;
        FVariant.dataInteger := ShortInt(ReadByte());
      end;
    $D1: // int16
      begin
        FType := mptInteger;
        FVariant.dataInteger := SmallInt(ReadBEWord());
      end;
    $D2: // int32
      begin
        FType := mptInteger;
        FVariant.dataInteger := Integer(ReadBEDWord());
      end;
    $D3: // int64
      begin
        FType := mptInteger;
        FVariant.dataInteger := ReadBEQWord();
      end;
    $DA: // raw16
      begin
        FType := mptRaw;
        FBytes := ReadBytes(ReadBEWord());
      end;
    $DB: // raw32
      begin
        FType := mptRaw;
        FBytes := ReadBytes(ReadBEDWord());
      end;
    $DC: // array16
      begin
        FType := mptArray;
        FVariant.dataArray := ReadArray(ReadBEWord());
      end;
    $DD: // array32
      begin
        FType := mptArray;
        FVariant.dataArray := ReadArray(ReadBEDWord());
      end;
    $DE: // map16
      begin
        FType := mptMap;
        FVariant.dataMap := ReadMap(ReadBEWord());
      end;
    $DF: // map32
      begin
        FType := mptMap;
        FVariant.dataMap := ReadMap(ReadBEDWord());
      end;
  else
    if prefix and $E0 = $E0 then // fix neg
    begin
      FType := mptInteger;
      FVariant.dataInteger := ShortInt(prefix);
    end
    else if prefix and $A0 = $A0 then // fix raw
    begin
      FType := mptRaw;
      FBytes := ReadBytes(prefix and (not $A0));
    end
    else if prefix and $90 = $90 then // fix array
    begin
      FType := mptArray;
      FVariant.dataArray := ReadArray(prefix and (not $90));
    end
    else if prefix and $80 = $80 then // fix map
    begin
      FType := mptMap;
      FVariant.dataMap := ReadMap(prefix and (not $80));
    end
    else if prefix shr 7 = 0 then // fix pos
    begin
      FType := mptInteger;
      FVariant.dataInteger := prefix;
    end
    else
      raise EInvalidOperation.CreateFmt('Unknown prefix \x%.2x', [prefix]);
  end;
end;

constructor TMsgPackObject.Parse(const Stream: TStream);
begin
  Read(Stream);
end;

function TMsgPackObject.Clone(): IMsgPackObject;
var
  i: Integer;
  mapIt: TMsgPackMapIterator;
begin
  case FType of
    mptNil:
      Result := TMsgPackObject.Create(mptNil);
    mptBoolean:
      Result := TMsgPackObject.Create(FVariant.dataBoolean);
    mptInteger:
      Result := TMsgPackObject.Create(FVariant.dataInteger);
    mptFloat:
      Result := TMsgPackObject.Create(FVariant.dataFloat);
    mptDouble:
      Result := TMsgPackObject.Create(FVariant.dataDouble);
    mptRaw:
      Result := TMsgPackObject.Create(FBytes);
    mptArray:
      begin
        Result := TMsgPackObject.Create(mptArray);
        for i := 0 to FVariant.dataArray.Count - 1 do
          Result.AsArray().Add(FVariant.dataArray[i].Clone())
      end;
    mptMap:
      begin
        Result := TMsgPackObject.Create(mptMap);
        FVariant.dataMap.IteratorInit(mapIt);
        while FVariant.dataMap.IteratorAdvance(mapIt) do
          Result.AsMap().Put(mapIt.Key, mapIt.Value.Clone());
      end;
  end;
end;

initialization

  DeletedBucket := TMsgPackObject.Create;

end.

