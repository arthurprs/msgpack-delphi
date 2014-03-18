program test;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  Windows,
  SysUtils,
  Classes,
  MsgPack in '../msgpack.pas';


procedure TestMsgPack();
const
  // some ints
  Ints: array[0..23] of Int64 = (32216, Low(Int64), Low(Integer), Low(SmallInt), Low(ShortInt), -100, -10, -1, -2, 0,
    1, 10, 100, 200, 1000, 3000, 30000, 300000, 3000000, High(ShortInt), High(SmallInt), High(Integer), High(cardinal),
    High(Int64));
var
  ob: IMsgPackObject;
  ar: TMsgPackArray;
  map: TMsgPackMap;
  s: Single;
  d: Double;
  i: Integer;
  n: Integer;
  nn: Int64;
begin
  // test array with some ints
  ob := TMsgPackObject.Create(mptArray);
  ar := ob.AsArray();
  for i := 0 to High(Ints) do
    ar.Add(MPO(Ints[i]));

  // test clone
  Assert(ob.Clone().AsMsgPack() = ob.AsMsgPack());

  // parse the dump and check the integrity
  ob := TMsgPackObject.Parse(ob.AsMsgPack());
  ar := ob.AsArray();
  for i := 0 to High(Ints) do
    Assert(ar[i].AsInteger = Ints[i], IntToStr(i) + ' : ' + IntToStr(Ints[i]) + ' ' + IntToStr(ar[i].AsInteger()));

  // test double and single in a map
  s := Random;
  d := Random;
  ob := TMsgPackObject.Create(mptMap);
  map := ob.AsMap();
  map['single'] := MPO(s);
  map['double'] := MPO(d);

  ob := TMsgPackObject.Parse(ob.AsMsgPack());
  map := ob.AsMap();
  Assert(s = map['single'].AsFloat(), FloatToStr(s) + ' ' + FloatToStr(map['single'].AsFloat()));
  Assert(d = map['double'].AsDouble(), FloatToStr(d) + ' ' + FloatToStr(map['double'].AsDouble()));

  // test clone
  Assert(ob.Clone().AsMsgPack() = ob.AsMsgPack());

  // test map
  ob := TMsgPackObject.Create(mptMap);
  map := ob.AsMap();
  for i := 0 to 100000 do
  begin
    map['key' + IntToStr(i)] := MPO('value' + IntToStr(i));
    if i mod 31 = 0 then // i don't like multiples of 31, delete them :O
      map.Delete('key' + IntToStr(i));
  end;

  ob := TMsgPackObject.Parse(ob.AsMsgPack());
  map := ob.AsMap();
  for i := 0 to 100000 do
  begin
    if i mod 31 = 0 then
      Assert(map['key' + IntToStr(i)] = nil) // make sure they're not there
    else
      Assert(map['key' + IntToStr(i)].AsString() = 'value' + IntToStr(i));
  end;

  // make sure the int handling is good
  RandSeed := GetTickCount();
  for i := 0 to 1000000 do
  begin
    n := Random(Integer(-1));
    Assert(n = MPOFromMsgPack(MPO(n).AsMsgPack()).AsInteger,
      IntToStr(n) + ' ' + IntToStr(MPOFromMsgPack(MPO(n).AsMsgPack()).AsInteger));
  end;

  for i := 0 to 1000000 do
  begin
    nn := (Int64(Random(Integer(-1))) shl 32) or Int64(Random(Integer(-1)));
    Assert(nn = MPOFromMsgPack(MPO(nn).AsMsgPack()).AsInteger,
      IntToStr(nn) + ' ' + IntToStr(MPOFromMsgPack(MPO(nn).AsMsgPack()).AsInteger));
  end;

  Writeln('Test passed');
end;

begin
  try
    TestMsgPack();
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
  Readln;
end.
