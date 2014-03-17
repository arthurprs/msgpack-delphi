program bench;

{$APPTYPE CONSOLE}

uses
  Windows,
  SysUtils,
  superobject,
  msgpack in '..\msgpack.pas';

function GetTick: LongWord;
var
  tick, freq: TLargeInteger;
begin
  QueryPerformanceFrequency(freq);
  QueryPerformanceCounter(tick);
  Result := Trunc((tick / freq) * 1000);
end;

procedure BenchJson();
var
  js: ISuperObject;
  xs: ISuperObject;
  i, l: Integer;
  k: cardinal;
  s: SOString;
  json : SOString;
  ts: TSuperTableString;
  a: TSuperArray;
begin
  Randomize;
  js := TSuperObject.Create;
  ts := js.AsObject;
  k := GetTick;
  for i := 1 to 100000 do
  begin
    l := i * 33;
    s := 'param' + IntToStr(l);
    ts.s[s] := s;
    s := 'int' + IntToStr(l);
    ts.i[s] := i;
  end;
  k := GetTick - k;
  Writeln('insert map: ', k);

  k := GetTick();

  ts.O['array'] := TSuperObject.Create(stArray);
  a := ts.O['array'].AsArray;
  for i := 1 to 1000000 do
    a.Add(i);

  k := GetTick - k;
  Writeln('insert array: ', k);

  k := GetTick;
  json := js.AsJSon();
  Writeln('dump: ', GetTick - k);
  Writeln('size unicode: ', Length(json) * 2);
  Writeln('size utf8: ', Length(UTF8Encode(json)));

  k := GetTick;
  xs := TSuperObject.ParseString(PSOChar(json), False);
  Writeln('parse: ', GetTick - k);

  k := GetTick;
  ts := xs.AsObject;
  for i := 1 to 100000 * 2 do
  begin
    l := i * 33;
    s := 'param' + IntToStr(l);
    ts.s[s];

    l := i * 33;
    s := 'param' + IntToStr(l);
    ts.s[s];
  end;
  Writeln('acess map: ', GetTick - k);
end;

procedure BenchMsgPack();
var
  js: IMsgPackObject;
  xs: IMsgPackObject;
  i, l: Integer;
  k: cardinal;
  s: string;
  ts: TMsgPackMap;
  Data: RawByteString;
  a: TMsgPackArray;
begin
  Randomize;
  k := GetTick;
  js := TMsgPackObject.Create(mptMap);
  ts := js.AsMap;
  for i := 1 to 100000 do
  begin
    l := i * 33;
    s := 'param' + IntToStr(l);
    ts.Put(s, TMsgPackObject.Create(s));
    s := 'int' + IntToStr(l);
    ts.Put(s, TMsgPackObject.Create(i));
  end;
  Writeln('insert map:', GetTick - k);

  k := GetTick();
  ts.Put('array', TMsgPackObject.Create(mptArray));
  a := ts['array'].AsArray();
  for i := 1 to 1000000 do
    a.Add(TMsgPackObject.Create(i * 33));
  Writeln('insert array:', GetTick - k);

  k := GetTick;
  Data := js.AsMsgPack();
  Writeln('dump: ', GetTick - k);
  Writeln('size: ', Length(Data));
  
  k := GetTick;
  xs := TMsgPackObject.Parse(Data);
  Writeln('parse: ', GetTick - k);

  k := GetTick;
  ts := xs.AsMap;
  for i := 1 to 100000 * 2 do
  begin
    l := i * 33;
    s := 'param' + IntToStr(l);
    ts.Get(s);

    l := i * 33;
    s := 'param' + IntToStr(l);
    ts.Get(s);
  end;
  Writeln('access map: ', GetTick - k);
end;

var
  k : Cardinal;
begin
  try
    Writeln('--- Json ---');
    k := GetTick;
    BenchJson();
    Writeln('total + cleanup: ', GetTick - k);


    Writeln('--- MsgPack ---');
    k := GetTick;
    BenchMsgPack();
    Writeln('total + cleanup: ', GetTick - k);

  except
    on E: Exception do
    begin
      Writeln(E.Message);
    end;
  end;
  readln;

end.

